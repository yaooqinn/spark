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
 * 1. Caches the CTE plan via [[CacheManager]] as an in-memory columnar table
 * 2. Replaces all [[CTERelationRef]] nodes with the cached [[InMemoryRelation]]
 *
 * Note: This is a side-effecting optimizer rule -- it mutates the CacheManager during
 * optimization. Ideally, caching would happen in a QueryExecution hook between optimization
 * and physical planning, but that would require modifying QueryExecution.scala which is
 * higher-risk. This trade-off is acceptable for a POC; a future refactor could move the
 * cacheQuery() call to QueryExecution.
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
        val cteCache = mutable.HashMap.empty[Long, LogicalPlan]
        replaceWithCache(plan, cteCache)
    }
  }

  private def replaceWithCache(
      plan: LogicalPlan,
      cteCache: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {

    case WithCTE(child, cteDefs) =>
      val cacheManager = session.sharedState.cacheManager
      val storageLevel = conf.defaultCacheStorageLevel
      cteDefs.foreach { cteDef =>
        val processedChild = replaceWithCache(cteDef.child, cteCache)

        // Non-deterministic CTEs should use repartition, not cache
        if (!cteDef.deterministic) {
          val fallback = if (cteDef.underSubquery) {
            processedChild
          } else {
            RepartitionByExpression(Seq.empty, processedChild, None)
          }
          cteCache.put(cteDef.id, fallback)
        } else {
          // Check if this CTE's plan is already cached (supports reuse across queries)
          val existing = cacheManager.lookupCachedData(session, processedChild)
          if (existing.isEmpty) {
            val tableName = Some(s"cte_${cteDef.id}")
            cacheManager.cacheQuery(
              session, processedChild, tableName,
              storageLevel)
          }

          // Look up the InMemoryRelation for this CTE
          cacheManager.lookupCachedData(session, processedChild) match {
            case Some(cd) =>
              cteCache.put(cteDef.id, cd.cachedRepresentation)
            case None =>
              // Cache lookup failed -- fall back to repartition
              val fallback = if (cteDef.underSubquery) {
                processedChild
              } else {
                RepartitionByExpression(Seq.empty, processedChild, None)
              }
              cteCache.put(cteDef.id, fallback)
          }
        }
      }
      replaceWithCache(child, cteCache)

    case ref: CTERelationRef =>
      cteCache.get(ref.cteId) match {
        case Some(cachedPlan) =>
          val newPlan = cachedPlan match {
            case imr: InMemoryRelation => imr.newInstance()
            case other => other
          }
          if (ref.outputSet == newPlan.outputSet) {
            newPlan
          } else {
            require(ref.output.length == newPlan.output.length,
              s"CTE ref output length (${ref.output.length}) != " +
                s"cached plan output length (${newPlan.output.length})")
            val projectList = ref.output.zip(newPlan.output).map {
              case (tgtAttr, srcAttr) =>
                Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
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
