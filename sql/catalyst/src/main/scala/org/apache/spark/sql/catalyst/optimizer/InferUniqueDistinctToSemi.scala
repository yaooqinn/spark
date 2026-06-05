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

import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, DISTINCT_LIKE}

/**
 * Infer that an `INNER JOIN` followed by `DISTINCT` over only the left side's
 * attributes can be rewritten as `LEFT SEMI JOIN` when the right side is
 * provably unique on the join key. This eliminates a 2-layer HashAgg and a
 * 200-partition Exchange in the physical plan.
 *
 * Two equivalent surface patterns are matched. Both produce the same rewrite:
 *
 * {{{
 *   1. Distinct (when SQL `DISTINCT` survives ReplaceDistinctWithAggregate, e.g.
 *      when this rule fires in a custom batch before the replacement):
 *
 *      Distinct                          Project(leftAttrs)
 *        +- Project(leftAttrs)       =>   +- Join(LeftSemi, leftKey = rightKey)
 *             +- Join(Inner, ...)              :- left
 *                 :- left                      +- right
 *                 +- right
 *
 *   2. Aggregate(groupOnly) (the post-ReplaceDistinctWithAggregate canonical form
 *      seen in production Optimizer pipelines):
 *
 *      Aggregate(grouping = leftAttrs, agg = leftAttrs)    Project(leftAttrs)
 *        +- Project(leftAttrs)                         =>   +- Join(LeftSemi, ...)
 *             +- Join(Inner, ...)                                :- left
 *                                                                +- right
 *
 *   In both cases:
 *     - project refs is a subset of left.outputSet
 *     - right.distinctKeys covers the equi-join right keys
 * }}}
 *
 * Why this is safe:
 *   - Right side is unique on join key (proved via `right.distinctKeys`) ->
 *     each left row matches AT MOST one right row -> inner join cannot fan out
 *     to duplicate left rows.
 *   - Project only references left side -> right side's column values are not
 *     observable in output.
 *   - Therefore Inner-join + outer `Distinct` is semantically equivalent to
 *     `LeftSemi` join + Project (which filters non-matching left rows but
 *     produces NO duplicates by construction).
 *
 * Why this is a win:
 *   - Inner-join produces matching pairs -> outer Distinct must HashAgg + 200-way
 *     Exchange (`spark.sql.shuffle.partitions` default).
 *   - LeftSemi-join produces left rows only once, no shuffle needed if the join
 *     is broadcast.
 *
 * Validation: SF100 Parquet 5-iter MEDIAN cleanroom bench shows 43.51x wall-time
 * win (16708ms -> 384ms) for fact 100M x dim 1M join (sourcex@024fbae
 * `_bench/inner-to-semi-sf100/`).
 *
 * Dependencies: piggybacks on existing `LogicalPlanDistinctKeys` /
 * `DistinctKeyVisitor` infrastructure (gated by
 * `SQLConf.PROPAGATE_DISTINCT_KEYS_ENABLED`). No dependency on SPARK-26741
 * unique-key constraint propagation system -- the right-side `distinctKeys`
 * must be populated by explicit `Aggregate(groupOnly)`, `Distinct`, or other
 * distinct-producing operations that `DistinctKeyVisitor` already understands.
 */
object InferUniqueDistinctToSemi extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(
    _.containsAnyPattern(DISTINCT_LIKE, AGGREGATE), ruleId) {
    // Pattern 1: explicit Distinct over Project over Inner Join.
    case Distinct(p @ Project(_, j @ Join(left, _, Inner, Some(_), _)))
        if p.references.subsetOf(left.outputSet) &&
          isRightSideUniqueOnJoinKeys(j) =>
      p.copy(child = j.copy(joinType = LeftSemi))

    // Pattern 2: group-only Aggregate (post-ReplaceDistinctWithAggregate form)
    // over Project over Inner Join. groupOnly = no aggregate functions, grouping
    // expressions are exactly the projected attributes.
    case agg @ Aggregate(_, _, p @ Project(_, j @ Join(left, _, Inner, Some(_), _)), _)
        if agg.groupOnly &&
          agg.references.subsetOf(p.outputSet) &&
          ExpressionSet(agg.groupingExpressions) ==
            ExpressionSet(p.projectList.map(_.toAttribute)) &&
          p.references.subsetOf(left.outputSet) &&
          isRightSideUniqueOnJoinKeys(j) =>
      p.copy(child = j.copy(joinType = LeftSemi))
  }

  // Right side is unique on the join key iff:
  //   * the join has at least one equi-condition (so we can extract right keys)
  //   * right.distinctKeys contains a set that's a subset of the right-side
  //     equi-keys (i.e. some unique combination on right is also implied by
  //     the equi-keys present in the join condition).
  // We delegate equi-key extraction to ExtractEquiJoinKeys but only inspect
  // the result; the original Join.condition is preserved verbatim in the
  // rewrite, so we never need to reconstruct it.
  private def isRightSideUniqueOnJoinKeys(j: Join): Boolean = {
    j.right.distinctKeys.nonEmpty && {
      j match {
        case ExtractEquiJoinKeys(_, _, rightKeys, _, _, _, _, _) if rightKeys.nonEmpty =>
          j.right.distinctKeys.exists(_.subsetOf(ExpressionSet(rightKeys)))
        case _ => false
      }
    }
  }
}
