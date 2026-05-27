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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * The HashedRelationContainsSubquery expression is injected on the probe side of a
 * broadcast hash join. Unlike the existing DPP DynamicPruningSubquery (which collects
 * distinct keys to the driver), HRC keeps the build-side HashedRelation broadcast
 * untouched and performs an O(1) `bc.value.contains(packedKey)` per probe row at
 * the physical layer via HashedRelationContainsExec + BroadcastedHashedRelationRef.
 *
 * The logical form is intentionally similar in spirit to DynamicPruningSubquery
 * (so the optimizer's SubqueryExpression plumbing applies for free), but distinct
 * in identity so that physical planning can rewrite into HRC-specific exec nodes.
 *
 * @param pruningKeys  the probe-side join keys (parallel to broadcastKeyIndices order;
 *                     composite-key packing via HashJoin.rewriteKeyExpr SSOT happens at
 *                     physical planning in PlanHashedRelationContainsFilters per
 *                     features/spark-hashed-relation-contains/docs/0007-investigation-p2c-1-composite-key-design.md)
 * @param buildQuery   the build-side subtree (subject to ReuseExchange / sameResult
 *                     reuse with the BHJ's own broadcast)
 * @param buildKeys    the build-side join keys; parallel to pruningKeys
 * @param broadcastKeyIndices indices of the filtering keys collected from the broadcast
 */
case class HashedRelationContainsSubquery(
    pruningKeys: Seq[Expression],
    buildQuery: LogicalPlan,
    buildKeys: Seq[Expression],
    broadcastKeyIndices: Seq[Int],
    exprId: ExprId = NamedExpression.newExprId,
    hint: Option[HintInfo] = None)
  extends SubqueryExpression(buildQuery, pruningKeys, exprId, Seq.empty, hint)
  with Unevaluable {

  override def dataType: DataType = BooleanType

  override def plan: LogicalPlan = buildQuery

  override def nullable: Boolean = false

  override def withNewPlan(plan: LogicalPlan): HashedRelationContainsSubquery =
    copy(buildQuery = plan)

  override def withNewOuterAttrs(outerAttrs: Seq[Expression]): HashedRelationContainsSubquery = {
    assert(outerAttrs.size == pruningKeys.size)
    copy(pruningKeys = outerAttrs)
  }

  override def withNewHint(hint: Option[HintInfo]): SubqueryExpression = copy(hint = hint)

  override lazy val resolved: Boolean = {
    pruningKeys.nonEmpty &&
      pruningKeys.forall(_.resolved) &&
      buildQuery.resolved &&
      buildKeys.nonEmpty &&
      buildKeys.forall(_.resolved) &&
      broadcastKeyIndices.nonEmpty &&
      broadcastKeyIndices.size == pruningKeys.size &&
      broadcastKeyIndices.forall(idx => idx >= 0 && idx < buildKeys.size) &&
      buildKeys.forall(_.references.subsetOf(buildQuery.outputSet)) &&
      pruningKeys.zip(broadcastKeyIndices).forall { case (pk, idx) =>
        pk.dataType == buildKeys(idx).dataType
      }
  }

  final override def nodePatternsInternal(): Seq[TreePattern] =
    Seq(HASHED_RELATION_CONTAINS_SUBQUERY)

  override def toString: String = s"hashedrelationcontains#${exprId.id} $conditionString"

  override lazy val canonicalized: HashedRelationContainsSubquery = {
    copy(
      pruningKeys = pruningKeys.map(_.canonicalized),
      buildQuery = buildQuery.canonicalized,
      buildKeys = buildKeys.map(QueryPlan.normalizeExpressions(_, buildQuery.output)),
      exprId = ExprId(0))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): HashedRelationContainsSubquery =
    copy(pruningKeys = newChildren)
}
