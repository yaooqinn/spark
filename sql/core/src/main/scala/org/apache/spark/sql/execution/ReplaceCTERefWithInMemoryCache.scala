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

import org.apache.spark.sql.catalyst.expressions.{Alias, Or, SubqueryExpression}
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
 * 1. Reconstructs a cache plan from the original plan and pushed predicates:
 *    `Filter(pushed_predicates, original_plan)` with ALL columns (no column pruning).
 *    This preserves predicate pushdown efficiency (scan/join/agg process only filtered data)
 *    while keeping all columns available for cross-query reuse.
 * 2. Uses the cache plan (including predicates) as the cache key. Same predicates across
 *    queries produce cache hits; different predicates produce separate cache entries,
 *    preventing data correctness issues.
 * 3. Replaces [[CTERelationRef]] nodes with cached [[InMemoryRelation]], using name-based
 *    column matching (since the cache has all columns but CTE refs may be column-pruned).
 *
 * Correctness: [[PushdownPredicatesAndPruneColumnsForCTEDef]] pushes predicates INTO the CTE
 * definition but does NOT remove the original predicates from above the CTE reference site.
 * So the outer filters remain as the correctness guarantee.
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

  private case class CachedCTEInfo(plan: LogicalPlan)

  /**
   * Build the plan to cache from originalPlanWithPredicates:
   * - Keep pushed predicates (for scan/join/agg efficiency)
   * - Remove column pruning (keep all columns for cross-query reuse)
   *
   * If predicates were pushed: Filter(pred1 OR pred2, original_plan)
   * If no predicates: original_plan as-is
   */
  private def buildCachePlan(cteDef: CTERelationDef): LogicalPlan = {
    cteDef.originalPlanWithPredicates match {
      case Some((originalPlan, preds)) if preds.nonEmpty =>
        Filter(preds.reduce(Or), originalPlan)
      case Some((originalPlan, _)) =>
        originalPlan
      case None =>
        // No pushdown happened — use the current child as-is
        cteDef.child
    }
  }

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
          // Build cache plan: Filter(pushed_preds, original_plan) with ALL columns.
          // The cache key includes predicates so different predicates produce separate
          // cache entries (correctness), while column pruning is stripped so different
          // column needs share the same entry (cross-query reuse).
          val cachePlan = replaceWithCache(buildCachePlan(cteDef), cteCache)

          // Check if this plan is already cached
          val existing = cacheManager.lookupCachedData(session, cachePlan)
          if (existing.isEmpty) {
            val tableName = Some(s"cte_${cteDef.id}")
            cacheManager.cacheQuery(session, cachePlan, tableName, storageLevel)
          }

          // Look up the InMemoryRelation for this CTE
          cacheManager.lookupCachedData(session, cachePlan) match {
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
            // Name-based column matching: the cached plan has all columns but the
            // CTE reference may be column-pruned. Select matching columns by name.
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
