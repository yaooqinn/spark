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
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * Caches non-inlined CTE definitions as [[InMemoryRelation]] columnar tables.
 *
 * For each [[WithCTE]] node, deterministic CTEs that were kept by [[InlineCTE]]
 * (via the `isCostlyToRepeat` heuristic) are cached through [[CacheManager]].
 * Each [[CTERelationRef]] is then replaced with the cached [[InMemoryRelation]].
 *
 * CTEs created by [[MergeSubplans]] (with `underSubquery = true`) are skipped
 * and left for [[ReplaceCTERefWithRepartition]] to handle via the repartition path.
 *
 * CTEs that fail to cache also fall through to [[ReplaceCTERefWithRepartition]].
 */
case class ReplaceCTERefWithInMemoryCache(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.CTE_CACHE_ENABLED) ||
        plan.isInstanceOf[Subquery] ||
        !plan.containsPattern(CTE)) {
      return plan
    }
    val cteCache = mutable.HashMap.empty[Long, LogicalPlan]
    replaceWithCache(plan, cteCache)
  }

  private def replaceWithCache(
      plan: LogicalPlan,
      cteCache: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {

    case WithCTE(child, cteDefs) =>
      val cacheManager = session.sharedState.cacheManager
      val storageLevel = conf.defaultCacheStorageLevel
      val minRefCount = conf.getConf(SQLConf.CTE_CACHE_MIN_REF_COUNT)
      // Count references per CTE in the outer plan to apply minRefCount threshold
      val refCounts = mutable.HashMap.empty[Long, Int].withDefaultValue(0)
      child.foreach {
        case ref: CTERelationRef => refCounts(ref.cteId) += 1
        case _ =>
      }
      val uncached = mutable.ArrayBuffer.empty[CTERelationDef]
      cteDefs.foreach { cteDef =>
        if (cteDef.deterministic && !cteDef.underSubquery &&
            refCounts(cteDef.id) >= minRefCount) {
          val processedChild = replaceWithCache(cteDef.child, cteCache)
          val existing = cacheManager.lookupCachedData(session, processedChild)
          if (existing.isEmpty) {
            cacheManager.cacheQuery(
              session, processedChild, Some(s"cte_${cteDef.id}"), storageLevel)
          }
          cacheManager.lookupCachedData(session, processedChild) match {
            case Some(cd) =>
              cteCache.put(cteDef.id, cd.cachedRepresentation)
            case None =>
              uncached += cteDef
          }
        } else {
          uncached += cteDef
        }
      }
      val newChild = replaceWithCache(child, cteCache)
      if (uncached.isEmpty) {
        newChild
      } else {
        WithCTE(newChild, uncached.toSeq)
      }

    case ref: CTERelationRef =>
      cteCache.get(ref.cteId).map { cachedPlan =>
        val newPlan = cachedPlan match {
          case imr: InMemoryRelation => imr.newInstance()
          case other => other
        }
        if (ref.outputSet == newPlan.outputSet) {
          newPlan
        } else {
          val projectList = ref.output.zip(newPlan.output).map {
            case (tgtAttr, srcAttr) =>
              Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
          }
          Project(projectList, newPlan)
        }
      }.getOrElse(ref)

    case other if other.containsPattern(CTE) =>
      other
        .withNewChildren(other.children.map(c => replaceWithCache(c, cteCache)))
        .transformExpressionsWithPruning(_.containsPattern(CTE)) {
          case e: SubqueryExpression if e.plan.containsPattern(CTE) =>
            e.withNewPlan(replaceWithCache(e.plan, cteCache))
        }

    case other => other
  }
}
