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

import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
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
 * For each non-inlined CTE definition, this rule:
 * 1. Caches the CTE's **original plan** (before predicate pushdown and column pruning) via
 *    [[CacheManager]] as an in-memory columnar table. This ensures cross-query cache reuse
 *    even when different queries push different predicates or prune different columns into the
 *    same CTE definition.
 * 2. Replaces all [[CTERelationRef]] nodes with the cached [[InMemoryRelation]], using
 *    name-based column matching (since the original plan may have more columns than a
 *    pruned CTE reference expects).
 *
 * Correctness: [[PushdownPredicatesAndPruneColumnsForCTEDef]] pushes predicates INTO the CTE
 * definition as an optimization but does NOT remove the original predicates from above the CTE
 * reference site. So caching the full (unpruned) plan is safe -- outer filters remain as the
 * correctness guarantee.
 *
 * This rule runs before [[CleanUpTempCTEInfo]] so that `originalPlanWithPredicates` is still
 * available on [[CTERelationDef]] nodes.
 *
 * When the config is disabled, this rule returns the plan unchanged and lets
 * [[ReplaceCTERefWithRepartition]] (the next rule in the batch) handle CTE replacement.
 */
case class ReplaceCTERefWithInMemoryCache(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.CTE_CACHE_ENABLED)) {
      return plan  // Let ReplaceCTERefWithRepartition handle it
    }
    plan match {
      case _: Subquery => plan
      case _ if !plan.containsPattern(CTE) => plan
      case _ =>
        val cteCache = mutable.HashMap.empty[Long, CachedCTEInfo]
        replaceWithCache(plan, cteCache)
    }
  }

  /**
   * Info about a cached CTE: the InMemoryRelation (or fallback plan), plus the output
   * of the plan that was actually cached (the original plan's output, which may have
   * more columns than a pruned CTE reference expects).
   */
  private case class CachedCTEInfo(plan: LogicalPlan)

  private def replaceWithCache(
      plan: LogicalPlan,
      cteCache: mutable.HashMap[Long, CachedCTEInfo]): LogicalPlan = plan match {

    case WithCTE(child, cteDefs) =>
      val cacheManager = session.sharedState.cacheManager
      val storageLevel = conf.defaultCacheStorageLevel
      cteDefs.foreach { cteDef =>
        // Non-deterministic CTEs should use repartition, not cache
        if (!cteDef.deterministic) {
          val processedChild = replaceWithCache(cteDef.child, cteCache)
          val fallback = if (cteDef.underSubquery) {
            processedChild
          } else {
            RepartitionByExpression(Seq.empty, processedChild, None)
          }
          cteCache.put(cteDef.id, CachedCTEInfo(fallback))
        } else {
          // Use the ORIGINAL plan (before predicate pushdown and column pruning) as both
          // the cache key and the cached data. This ensures:
          // 1. Cross-query cache reuse: different queries pushing different predicates/columns
          //    into the same CTE will find the same cache entry.
          // 2. Correctness: outer filters (above CTE refs) are preserved by
          //    PushdownPredicatesAndPruneColumnsForCTEDef, so they handle row filtering.
          // 3. Column selection: InMemoryRelation scans select only needed columns.
          val originalPlan = cteDef.originalPlanWithPredicates match {
            case Some((origPlan, _)) => origPlan
            case None => cteDef.child
          }
          val cacheKeyPlan = replaceWithCache(originalPlan, cteCache)

          // Check if this CTE's original plan is already cached
          val existing = cacheManager.lookupCachedData(session, cacheKeyPlan)
          if (existing.isEmpty) {
            val tableName = Some(s"cte_${cteDef.id}")
            cacheManager.cacheQuery(session, cacheKeyPlan, tableName, storageLevel)
          }

          // Look up the InMemoryRelation for this CTE
          cacheManager.lookupCachedData(session, cacheKeyPlan) match {
            case Some(cd) =>
              cteCache.put(cteDef.id, CachedCTEInfo(cd.cachedRepresentation))
            case None =>
              // Cache lookup failed -- fall back to repartition
              val processedChild = replaceWithCache(cteDef.child, cteCache)
              val fallback = if (cteDef.underSubquery) {
                processedChild
              } else {
                RepartitionByExpression(Seq.empty, processedChild, None)
              }
              cteCache.put(cteDef.id, CachedCTEInfo(fallback))
          }
        }
      }
      replaceWithCache(child, cteCache)

    case ref: CTERelationRef =>
      cteCache.get(ref.cteId) match {
        case Some(CachedCTEInfo(cachedPlan)) =>
          val newPlan = cachedPlan match {
            case imr: InMemoryRelation => imr.newInstance()
            case other => other
          }
          if (ref.outputSet == newPlan.outputSet) {
            newPlan
          } else {
            // Name-based column matching: the cached plan may have more columns than
            // the pruned CTE reference (due to column pruning by
            // PushdownPredicatesAndPruneColumnsForCTEDef). We select matching columns
            // by name to handle this correctly.
            val cachedOutputByName = newPlan.output.map(a => a.name -> a).toMap
            val projectList = ref.output.map { tgtAttr =>
              cachedOutputByName.get(tgtAttr.name) match {
                case Some(srcAttr) =>
                  Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
                case None =>
                  throw new IllegalStateException(
                    s"CTE ref column '${tgtAttr.name}' not found in cached plan output: " +
                      s"[${newPlan.output.map(_.name).mkString(", ")}]")
              }
            }
            Project(projectList, newPlan)
          }
        case None => ref
      }

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
}
