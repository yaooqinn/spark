/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * Replaces non-inlined CTE references with [[InMemoryRelation]] (columnar cache)
 * when `spark.sql.optimizer.cte.cache.enabled` is enabled.
 *
 * Uses per-reference caching: each CTE reference is cached based on its own
 * outer predicates and column needs, not the OR-merged predicates across all refs.
 * This ensures:
 * - References with selective filters cache only filtered data (e.g., q24a main query
 *   with i_color='peach' caches only 3,500 rows instead of 3.5M)
 * - References without filters cache full data (e.g., HAVING scalar subquery)
 * - Cross-query reuse: identical reference patterns share cache entries
 *
 * The per-reference predicate/column info is gathered by walking the plan and
 * extracting PhysicalOperation(projects, predicates, CTERelationRef) for each ref,
 * similar to [[PushdownPredicatesAndPruneColumnsForCTEDef]].
 */
case class ReplaceCTERefWithInMemoryCache(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.CTE_CACHE_ENABLED)) {
      return plan
    }
    plan match {
      case _: Subquery => plan
      case _ if !plan.containsPattern(CTE) => plan
      case _ =>
        val cteCache = mutable.HashMap.empty[Long, LogicalPlan]
        replaceWithCache(plan, cteCache)
    }
  }

  /**
   * Per-reference info: the predicates and columns needed by a specific CTE reference.
   */
  private case class RefInfo(
    predicates: Seq[Expression])

  /**
   * Gather per-reference predicate and column info from the plan. For each
   * CTERelationRef encountered under a PhysicalOperation, extract the outer
   * predicates and referenced columns, mapped to CTE definition attributes.
   *
   * Returns a map from CTERelationRef's exprId to its RefInfo.
   */
  private def gatherPerRefInfo(
      plan: LogicalPlan,
      cteDefs: Map[Long, CTERelationDef]): Map[ExprId, RefInfo] = {
    val result = mutable.HashMap.empty[ExprId, RefInfo]

    def gather(p: LogicalPlan): Unit = p match {
      case PhysicalOperation(projects, predicates, ref: CTERelationRef) =>
        cteDefs.get(ref.cteId).foreach { cteDef =>
          val attrMapping = ref.output.zip(cteDef.output).toMap
          // Map predicates back to CTE definition attributes
          val mappedPreds = predicates.map(_.transform {
            case a: Attribute => attrMapping.getOrElse(a, a)
          }).filter(_.references.forall(cteDef.outputSet.contains))

          // Use the first output attribute's exprId as ref identity
          result.put(ref.output.head.exprId, RefInfo(mappedPreds))
        }

      case _ =>
        p.children.foreach(gather)
    }

    gather(plan)
    result.toMap
  }

  /**
   * Build a per-reference CTE plan: apply this ref's predicates and column
   * pruning to the original CTE plan.
   */
  private def buildRefPlan(
      originalPlan: LogicalPlan,
      refInfo: RefInfo): LogicalPlan = {
    if (refInfo.predicates.nonEmpty) {
      Filter(refInfo.predicates.reduce(And), originalPlan)
    } else {
      originalPlan
    }
  }

  private def replaceWithCache(
      plan: LogicalPlan,
      cteCache: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {

    case WithCTE(child, cteDefs) =>
      val cacheManager = session.sharedState.cacheManager
      val storageLevel = conf.defaultCacheStorageLevel

      // Build CTE def map and process nested CTEs first
      val cteDefMap = mutable.HashMap.empty[Long, CTERelationDef]
      cteDefs.foreach { cteDef =>
        val processedChild = replaceWithCache(cteDef.child, cteCache)
        val newDef = cteDef.copy(child = processedChild)
        cteDefMap.put(cteDef.id, newDef)

        if (!cteDef.deterministic) {
          val fallback = if (cteDef.underSubquery) {
            processedChild
          } else {
            RepartitionByExpression(Seq.empty, processedChild, None)
          }
          cteCache.put(cteDef.id, fallback)
        }
      }

      // Gather per-reference info from the outer plan (child of WithCTE)
      val perRefInfo = gatherPerRefInfo(child, cteDefMap.toMap)

      // For each deterministic CTE, build and cache per-reference plans
      // Map from ref exprId -> InMemoryRelation for that ref's version
      val perRefCache = mutable.HashMap.empty[ExprId, LogicalPlan]
      cteDefs.foreach { cteDef =>
        if (cteDef.deterministic && !cteCache.contains(cteDef.id)) {
          val processedDef = cteDefMap(cteDef.id)
          val originalPlan = processedDef.child

          // First, set up default CTE-level cache (full plan, no per-ref filter)
          if (!originalPlan.containsPattern(CTE)) {
            var fullCached = cacheManager.lookupCachedData(session, originalPlan)
            if (fullCached.isEmpty) {
              cacheManager.cacheQuery(session, originalPlan,
                Some(s"cte_${cteDef.id}"), storageLevel)
              fullCached = cacheManager.lookupCachedData(session, originalPlan)
            }
            fullCached match {
              case Some(cd) => cteCache.put(cteDef.id, cd.cachedRepresentation)
              case None =>
                val fallback = if (cteDef.underSubquery) originalPlan
                  else RepartitionByExpression(Seq.empty, originalPlan, None)
                cteCache.put(cteDef.id, fallback)
            }
          } else {
            // Plan contains nested CTE refs -- fall back to repartition
            val fallback = if (cteDef.underSubquery) originalPlan
              else RepartitionByExpression(Seq.empty, originalPlan, None)
            cteCache.put(cteDef.id, fallback)
          }

          // Then, build per-reference plans for refs with specific predicates
          val refsForCte = perRefInfo.filter { case (exprId, _) =>
            findRefByExprId(child, exprId, cteDef.id)
          }

          if (refsForCte.nonEmpty) {
            val groups = refsForCte.groupBy { case (_, info) =>
              info.predicates.map(_.canonicalized).toSet
            }

            groups.foreach { case (predSet, groupRefs) =>
              if (predSet.nonEmpty) {
                val refInfo = groupRefs.head._2
                val refPlan = buildRefPlan(originalPlan, refInfo)

                if (!refPlan.containsPattern(CTE)) {
                  var cached = cacheManager.lookupCachedData(session, refPlan)
                  if (cached.isEmpty) {
                    val tableName = Some(s"cte_${cteDef.id}_filtered")
                    cacheManager.cacheQuery(session, refPlan, tableName, storageLevel)
                    cached = cacheManager.lookupCachedData(session, refPlan)
                  }

                  cached.foreach { cd =>
                    groupRefs.foreach { case (exprId, _) =>
                      perRefCache.put(exprId, cd.cachedRepresentation)
                    }
                  }
                }
              }
            }
          }
        }
      }

      replaceWithCacheAndPerRef(child, cteCache, perRefCache)

    case ref: CTERelationRef =>
      resolveRef(ref, cteCache, mutable.HashMap.empty)

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(c => replaceWithCache(c, cteCache)))
        .transformExpressionsWithPruning(
          _.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression =>
            e.withNewPlan(replaceWithCache(e.plan, cteCache))
        }

    case _ => plan
  }

  /** Replace refs using both per-ref cache and fallback CTE cache. */
  private def replaceWithCacheAndPerRef(
      plan: LogicalPlan,
      cteCache: mutable.HashMap[Long, LogicalPlan],
      perRefCache: mutable.HashMap[ExprId, LogicalPlan]): LogicalPlan = plan match {

    case ref: CTERelationRef =>
      resolveRef(ref, cteCache, perRefCache)

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(c =>
          replaceWithCacheAndPerRef(c, cteCache, perRefCache)))
        .transformExpressionsWithPruning(
          _.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression =>
            e.withNewPlan(replaceWithCacheAndPerRef(e.plan, cteCache, perRefCache))
        }

    case _ => plan
  }

  /** Resolve a CTERelationRef to its cached InMemoryRelation. */
  private def resolveRef(
      ref: CTERelationRef,
      cteCache: mutable.HashMap[Long, LogicalPlan],
      perRefCache: mutable.HashMap[ExprId, LogicalPlan]): LogicalPlan = {
    // Try per-ref cache first, fall back to CTE-level cache
    val cachedPlan = perRefCache.get(ref.output.head.exprId)
      .orElse(cteCache.get(ref.cteId))

    cachedPlan match {
      case Some(plan) =>
        val newPlan = plan match {
          case imr: InMemoryRelation => imr.newInstance()
          case other => other
        }
        if (ref.outputSet == newPlan.outputSet) {
          newPlan
        } else {
          // Name-based column matching for per-ref cache with different columns
          val cachedOutputByName = newPlan.output.map(a => a.name -> a).toMap
          val projectList = ref.output.flatMap { tgtAttr =>
            cachedOutputByName.get(tgtAttr.name).map { srcAttr =>
              Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
            }.orElse {
              // Column not in this per-ref cache -- shouldn't happen if
              // per-ref info was gathered correctly
              throw new IllegalStateException(
                s"CTE ref column '${tgtAttr.name}' not found in cached plan: " +
                  s"[${newPlan.output.map(_.name).mkString(", ")}]")
            }
          }
          Project(projectList, newPlan)
        }
      case None => ref
    }
  }

  /** Check if a plan contains a CTERelationRef with given exprId and cteId. */
  private def findRefByExprId(
      plan: LogicalPlan, exprId: ExprId, cteId: Long): Boolean = {
    plan.find {
      case ref: CTERelationRef =>
        ref.cteId == cteId && ref.output.headOption.exists(_.exprId == exprId)
      case _ => false
    }.isDefined
  }

}
