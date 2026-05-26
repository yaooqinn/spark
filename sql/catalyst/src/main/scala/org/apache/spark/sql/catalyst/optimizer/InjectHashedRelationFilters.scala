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

import org.apache.spark.sql.catalyst.expressions.{Expression, HashedRelationContainsSubquery, PredicateHelper}
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

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.runtimeFilterHashedRelationContainsEnabled) {
      return plan
    }
    plan.transformWithPruning(_.containsPattern(JOIN)) {
      case j @ ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, _, left, right, hint)
          if leftKeys.size == 1 && rightKeys.size == 1 =>
        // Try the right side as build (probe filter applied on the left).
        val withLeftProbe =
          maybeInjectProbe(j, leftKeys.head, rightKeys.head, left, right, buildIsRight = true)
        // Then try the left side as build (probe filter applied on the right) on the
        // possibly-rewritten join. We re-extract because the join structure may have
        // changed if the first inject site succeeded.
        withLeftProbe match {
          case j2 @ ExtractEquiJoinKeys(_, lk2, rk2, _, _, l2, r2, _)
              if lk2.size == 1 && rk2.size == 1 =>
            maybeInjectProbe(j2, rk2.head, lk2.head, r2, l2, buildIsRight = false)
          case other => other
        }
    }
  }

  private def maybeInjectProbe(
      join: Join,
      probeKey: Expression,
      buildKey: Expression,
      probePlan: LogicalPlan,
      buildPlan: LogicalPlan,
      buildIsRight: Boolean): LogicalPlan = {
    if (!canBroadcastBySize(buildPlan, conf)) return join
    if (canBroadcastBySize(probePlan, conf)) return join
    // Avoid re-injecting on a probe plan that already contains the same HRC subquery
    // for this build key (idempotence under FixedPoint(1) re-trigger).
    if (probePlan.exists {
      case Filter(cond, _) => cond.exists {
        case h: HashedRelationContainsSubquery =>
          h.buildKeys.headOption.exists(_.semanticEquals(buildKey))
        case _ => false
      }
      case _ => false
    }) {
      return join
    }
    val subquery = HashedRelationContainsSubquery(
      pruningKey = probeKey,
      buildQuery = buildPlan,
      buildKeys = Seq(buildKey),
      broadcastKeyIndices = Seq(0))
    val newProbe = Filter(subquery, probePlan)
    if (buildIsRight) {
      join.copy(left = newProbe)
    } else {
      join.copy(right = newProbe)
    }
  }
}
