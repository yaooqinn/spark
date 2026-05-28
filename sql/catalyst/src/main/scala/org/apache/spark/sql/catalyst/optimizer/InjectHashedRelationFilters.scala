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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, HashedRelationContainsSubquery, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * Injects a HashedRelation.contains runtime filter on the probe side of a
 * broadcast hash join, reusing the build-side HashedRelation through a new
 * subquery node that exposes the broadcast reference without driver-side
 * collect (see HashedRelationContainsSubquery /
 * BroadcastedHashedRelationRef / HashedRelationContainsExec for the four new
 * scaffolded classes that this rule eventually rewrites into).
 *
 * Detection (this slice): for each equi-join where one side is broadcastable
 * by size and the other is not, wrap the probe-side plan in a
 * Filter(HashedRelationContainsSubquery(probeKey, buildPlan, buildKeys, ...)).
 *
 * Physical rewrite into HashedRelationContainsExec + BroadcastedHashedRelationRef
 * (with sameResult reuse of the sibling BHJ's broadcast) lands in
 * PlanHashedRelationContainsFilters in the next slice.
 */
object InjectHashedRelationFilters extends Rule[LogicalPlan] with PredicateHelper
  with JoinSelectionHelper {

  /**
   * Returns true iff the probe plan already carries a Bloom-filter probe
   * (`BloomFilterMightContain`) whose key lineage overlaps any of the HRC
   * probe keys. Lineage match uses `ExprId`-strict equality, so an `Alias`
   * rename between scan and probe intentionally breaks the match.
   *
   * Gated by `RUNTIME_HASHED_RELATION_CONTAINS_BLOOM_MUTUAL_EXCLUSION`;
   * when the conf is `false`, returns `false` and the two runtime filters
   * coexist on the same probe site.
   */
  private[sql] def hasBloomOnSameScanLineage(
      probePlan: LogicalPlan,
      hrcProbeKeys: Seq[Expression]): Boolean = {
    if (!conf.runtimeFilterHashedRelationContainsBloomMutualExclusion) {
      return false
    }
    val bloomKeyAttrSets: Seq[Set[Long]] = probePlan.collect {
      case Filter(cond, _) => cond.collect {
        case bf: BloomFilterMightContain =>
          // bf.right is XxHash64(Seq(key)); its .references is the attr set.
          bf.right.references.map(_.exprId.id).toSet
      }
    }.flatten
    if (bloomKeyAttrSets.isEmpty) return false
    hrcProbeKeys.exists { hrcKey =>
      val hrcAttrIds = hrcKey.references.map(_.exprId.id).toSet
      bloomKeyAttrSets.exists(bloomIds => bloomIds.intersect(hrcAttrIds).nonEmpty)
    }
  }

  /**
   * Per-query inject budget. Mirrors peer
   * `InjectRuntimeFilter.tryInjectRuntimeFilter` (`var filterCounter` at L282
   * + `RUNTIME_FILTER_NUMBER_THRESHOLD` at L283): a single counter, scoped to
   * one `apply` invocation, increments after each successful inject and is
   * read by the cost-model gate at the next site.
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.runtimeFilterHashedRelationContainsEnabled) {
      return plan
    }
    var filterCounter = 0
    plan.transformWithPruning(_.containsPattern(JOIN)) {
      case j @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint)
          if leftKeys.size == rightKeys.size && leftKeys.nonEmpty =>
        // joinType gate (peer-parity with InjectRuntimeFilter L140/L159): only
        // inject on a probe side that is correctness-safe to prune. canPruneLeft
        // / canPruneRight from JoinSelectionHelper return false for joinTypes
        // where dropping rows on the corresponding side would change the answer
        // (e.g. LeftAnti, FullOuter, ExistenceJoin, NullAwareAntiJoin). Without
        // this gate the HRC predicate "key IN broadcast" silently drops rows
        // that LeftAnti is supposed to keep.
        // Try the right side as build (probe filter applied on the left).
        val (afterLeft, leftInjected) = if (canPruneLeft(joinType)) {
          maybeInjectProbe(j, leftKeys, rightKeys, left, right,
            buildIsRight = true, filterCounter)
        } else {
          (j: LogicalPlan, false)
        }
        if (leftInjected) filterCounter += 1
        // Then try the left side as build (probe filter applied on the right) on the
        // possibly-rewritten join. We re-extract because the join structure may have
        // changed if the first inject site succeeded.
        afterLeft match {
          case j2 @ ExtractEquiJoinKeys(jt2, lk2, rk2, _, _, l2, r2, _)
              if lk2.size == rk2.size && lk2.nonEmpty && canPruneRight(jt2) =>
            val (afterRight, rightInjected) =
              maybeInjectProbe(j2, rk2, lk2, r2, l2,
                buildIsRight = false, filterCounter)
            if (rightInjected) filterCounter += 1
            afterRight
          case other => other
        }
    }
  }

  /**
   * Returns `(possibly-rewritten plan, didInject)`. Caller is responsible for
   * incrementing the per-query `filterCounter` when `didInject = true`.
   */
  private def maybeInjectProbe(
      join: Join,
      probeKeys: Seq[Expression],
      buildKeys: Seq[Expression],
      probePlan: LogicalPlan,
      buildPlan: LogicalPlan,
      buildIsRight: Boolean,
      filterCounter: Int): (LogicalPlan, Boolean) = {
    val buildBroadcastable = canBroadcastBySize(buildPlan, conf)
    val probeBroadcastable = canBroadcastBySize(probePlan, conf)
    HashedRelationFilterCostModel.shouldInject(
      buildPlan, probePlan,
      buildBroadcastable, probeBroadcastable,
      filterCounter, conf) match {
      case skip: HashedRelationFilterCostModel.Skip =>
        logDebug(s"HRC cost-model Skip: ${skip.reason}")
        return (join, false)
      case _: HashedRelationFilterCostModel.Inject => // continue
    }
    // Skip HRC inject when the probe side already carries a Bloom filter on
    // overlapping scan lineage; gated by the SQLConf above. The Bloom probe
    // already covers the same membership check, and stacking both runtime
    // filters on the same broadcast pays redundant per-row cost.
    if (hasBloomOnSameScanLineage(probePlan, probeKeys)) {
      return (join, false)
    }
    // Avoid re-injecting on a probe plan that already contains the same HRC subquery
    // for this build-key set (idempotence under FixedPoint(1) re-trigger).
    if (probePlan.exists {
      case Filter(cond, _) => cond.exists {
        case h: HashedRelationContainsSubquery =>
          h.buildKeys.size == buildKeys.size &&
            h.buildKeys.zip(buildKeys).forall { case (a, b) => a.semanticEquals(b) }
        case _ => false
      }
      case _ => false
    }) {
      return (join, false)
    }
    val subquery = HashedRelationContainsSubquery(
      pruningKeys = probeKeys,
      buildQuery = buildPlan,
      buildKeys = buildKeys,
      broadcastKeyIndices = buildKeys.indices)
    val newProbe = Filter(subquery, probePlan)
    val newJoin = if (buildIsRight) {
      join.copy(left = newProbe)
    } else {
      join.copy(right = newProbe)
    }
    (newJoin, true)
  }
}
