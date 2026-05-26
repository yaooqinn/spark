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

package org.apache.spark.sql.execution.runtimefilter

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec}
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}

/**
 * The physical probe-side predicate that implements HRC.
 *
 * Mixes in ExecSubqueryExpression so that the standard subquery preparation
 * machinery (SparkPlan.prepareSubqueries) invokes updateResult() AFTER all
 * preparations (including ReuseExchangeAndSubquery). At that point `plan` has
 * been dedup-rewritten to point at the sibling BroadcastHashJoinExec's
 * BroadcastExchangeExec (via ReusedExchangeExec wrapper), so
 * plan.executeBroadcast returns the same Broadcast[HashedRelation] handle
 * that the join will probe -- zero second materialization.
 *
 * Field design (driven by docs/0004-investigation-peer-audit-pass.md):
 *  - plan: BaseSubqueryExec  -- loosened from BroadcastedHashedRelationRef to
 *      admit ReusedSubqueryExec wrap (ReuseExchangeAndSubquery line 67 calls
 *      sub.withNewPlan with a BaseSubqueryExec; could be the original ref or
 *      a ReusedSubqueryExec).
 *  - @transient var broadcast  -- populated in updateResult, captured into
 *      task closures, dereferenced on executors via .value. Catalyst nulls
 *      this in canonicalized for stable equality.
 *
 * MVP scope: CodegenFallback path. First-class doGenCode (KeyAccessor inline
 * lookup) is a later optimization slice.
 */
case class HashedRelationContainsExec(
    packedProbeKey: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    var broadcast: Broadcast[HashedRelation] = null)
  extends ExecSubqueryExpression
  with UnaryLike[Expression]
  with Predicate
  with CodegenFallback {

  override def child: Expression = packedProbeKey

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def updateResult(): Unit = {
    // BaseSubqueryExec lacks doExecuteBroadcast; BroadcastedHashedRelationRef
    // deliberately rejects executeBroadcast on itself (see its scaladoc) and
    // exposes the broadcast handle only via its .broadcast accessor, which
    // routes through child = BroadcastExchangeExec (or its reuse forward).
    // Pattern-match unwrap is the sanctioned access path; see
    // 0004-investigation-peer-audit-pass.md Gap D.
    broadcast = plan match {
      case ref: BroadcastedHashedRelationRef => ref.broadcast
      case ReusedSubqueryExec(ref: BroadcastedHashedRelationRef) => ref.broadcast
      case other =>
        throw SparkException.internalError(
          "HashedRelationContainsExec.plan must be BroadcastedHashedRelationRef " +
            s"(optionally wrapped by ReusedSubqueryExec); got ${other.getClass.getSimpleName}")
    }
  }

  override def withNewPlan(plan: BaseSubqueryExec): HashedRelationContainsExec =
    copy(plan = plan)

  override def eval(input: InternalRow): Any = {
    val key = packedProbeKey.eval(input)
    if (key == null) {
      // Null probe keys never join in inner / non-null-aware outer joins
      // (BHJ filters them out), so HRC returns false to skip them too.
      false
    } else {
      val relation = broadcast.value
      packedProbeKey.dataType match {
        case LongType =>
          relation.get(key.asInstanceOf[Long]) != null
        case _ =>
          // Fallback: key is an InternalRow (composite-key UnsafeRow packing
          // per P2c). Single-key non-Long-packable keys also hit this branch.
          relation.get(key.asInstanceOf[InternalRow]) != null
      }
    }
  }

  override lazy val canonicalized: HashedRelationContainsExec = copy(
    packedProbeKey = packedProbeKey.canonicalized,
    plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
    exprId = ExprId(0),
    broadcast = null)

  override protected def withNewChildInternal(newChild: Expression): HashedRelationContainsExec =
    copy(packedProbeKey = newChild)
}
