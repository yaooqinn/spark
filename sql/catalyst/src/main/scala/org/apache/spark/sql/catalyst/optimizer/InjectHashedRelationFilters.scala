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
   * P2c-2 mutex helper (testability per todos 0010-investigation; visibility
   * private[sql] for HelperUnit tests in package org.apache.spark.sql).
   *
   * Returns true iff the probe plan already carries a Bloom-filter probe
   * (BloomFilterMightContain) whose key lineage overlaps any of the HRC
   * probe keys, per 0009 rev4 section 3.1:
   *   - per-key independent walk (any-match defer if at least one HRC key
   *     shares lineage with the Bloom key)
   *   - ExprId-strict equality (Alias rename breaks lineage match, F6.2)
   *   - lineage = AttributeReference set of the Bloom key's XxHash64 args,
   *     intersected with HRC key's own attribute set
   *
   * Conf gate: when RUNTIME_HASHED_RELATION_CONTAINS_BLOOM_MUTUAL_EXCLUSION
   * is false, short-circuit to false (mutex axis disabled, coexist mode).
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
    // Per-key independent walk + any-match.
    hrcProbeKeys.exists { hrcKey =>
      val hrcAttrIds = hrcKey.references.map(_.exprId.id).toSet
      bloomKeyAttrSets.exists(bloomIds => bloomIds.intersect(hrcAttrIds).nonEmpty)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.runtimeFilterHashedRelationContainsEnabled) {
      return plan
    }
    plan.transformWithPruning(_.containsPattern(JOIN)) {
      case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, _, left, right, hint)
          if leftKeys.size == rightKeys.size && leftKeys.nonEmpty =>
        // Try the right side as build (probe filter applied on the left).
        val withLeftProbe =
          maybeInjectProbe(j, leftKeys, rightKeys, left, right, buildIsRight = true)
        // Then try the left side as build (probe filter applied on the right) on the
        // possibly-rewritten join. We re-extract because the join structure may have
        // changed if the first inject site succeeded.
        withLeftProbe match {
          case j2 @ ExtractEquiJoinKeys(_, lk2, rk2, _, _, l2, r2, _)
              if lk2.size == rk2.size && lk2.nonEmpty =>
            maybeInjectProbe(j2, rk2, lk2, r2, l2, buildIsRight = false)
          case other => other
        }
    }
  }

  private def maybeInjectProbe(
      join: Join,
      probeKeys: Seq[Expression],
      buildKeys: Seq[Expression],
      probePlan: LogicalPlan,
      buildPlan: LogicalPlan,
      buildIsRight: Boolean): LogicalPlan = {
    if (!canBroadcastBySize(buildPlan, conf)) return join
    if (canBroadcastBySize(probePlan, conf)) return join
    // P2c-2: Bloom mutex defer. If the probe side already carries a Bloom
    // filter on the same scan lineage as any HRC probe key, skip HRC inject
    // to avoid double-redundant runtime work. See hasBloomOnSameScanLineage
    // for spec (0009 rev4 section 3.1) and SQLConf-gated coexist override.
    if (hasBloomOnSameScanLineage(probePlan, probeKeys)) {
      return join
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
      return join
    }
    val subquery = HashedRelationContainsSubquery(
      pruningKeys = probeKeys,
      buildQuery = buildPlan,
      buildKeys = buildKeys,
      broadcastKeyIndices = buildKeys.indices)
    val newProbe = Filter(subquery, probePlan)
    if (buildIsRight) {
      join.copy(left = newProbe)
    } else {
      join.copy(right = newProbe)
    }
  }
}
