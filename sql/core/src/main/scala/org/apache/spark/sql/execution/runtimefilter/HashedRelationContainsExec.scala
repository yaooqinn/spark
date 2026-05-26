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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral, JavaCode, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
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
 * Codegen path (P2c-0, per decision rev9 D7): first-class doGenCode emits an
 * inline `relation.getValue(key) != null` lookup. The broadcast handle is
 * wired into the codegen context via ctx.addReferenceObj (mirrors BHJ
 * prepareBroadcast in BroadcastHashJoinExec.scala line 192-205) and the
 * HashedRelation is materialized as a mutable state via addMutableState
 * forceInline=true. The result-shape boilerplate mirrors
 * BloomFilterMightContain.doGenCode (line 106-121): single-expr boolean
 * predicate with null-safe guard on the probe key. The interpreted eval()
 * path is retained as a fallback (same pattern as BloomFilterMightContain).
 *
 * Composite-key probe path (UnsafeRow lookup, single-Long packing fallback)
 * lands in P2c-1 (docs/0007-investigation-p2c-1-composite-key-design.md).
 * The current codegen branch covers only the LongType packed-key path; the
 * eval() fallback at line 100-103 already handles InternalRow probe keys for
 * runtime correctness until P2c-1 extends doGenCode.
 */
case class HashedRelationContainsExec(
    packedProbeKey: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    var broadcast: Broadcast[HashedRelation] = null)
  extends ExecSubqueryExpression
  with UnaryLike[Expression]
  with Predicate {

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

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Codegen path for the LongType packed-key fast path (P2c-0). Composite
    // UnsafeRow probe keys hit the interpreted eval() fallback above until
    // P2c-1 extends this method. We deliberately collapse BHJ's
    // keyIsUnique branching here because HRC only cares about existence:
    // HashedRelation.getValue returns the first matched row (or null) which
    // is equivalent to (get(key) != null) for contains semantics. See
    // grep-verified contract in HashedRelation.scala line 43-72 trait + line
    // 1005-1023 LongHashedRelation + line 231-264 UnsafeHashedRelation:
    // every implementation returns null on no-match, single-row or first-of-
    // many on hit. Skeleton mirrors BloomFilterMightContain.doGenCode
    // line 106-121 (single-expr boolean predicate, null-safe).
    if (broadcast == null) {
      // Defensive: updateResult should have populated this before codegen.
      // Mirror BloomFilterMightContain behaviour on a null filter.
      return ev.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
    }
    if (packedProbeKey.dataType != LongType) {
      // Composite UnsafeRow path lands in P2c-1; until then, emit a per-row
      // call to this.eval(input) so generated Java still compiles and exercises
      // the interpreted fallback at line 109-119. Mirrors the CodegenFallback
      // pattern but scoped to the non-Long branch only.
      val thisRef = ctx.addReferenceObj("hrc", this, classOf[Expression].getName)
      val inputRow = ctx.INPUT_ROW
      return ev.copy(code = code"""
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = (Boolean) $thisRef.eval($inputRow);""",
        isNull = FalseLiteral)
    }
    val broadcastRef = ctx.addReferenceObj("broadcast", broadcast,
      classOf[Broadcast[HashedRelation]].getName)
    val clsName = classOf[HashedRelation].getName
    val relationTerm = ctx.addMutableState(clsName, "hrcRelation",
      v =>
        s"""
           | $v = (($clsName) $broadcastRef.value()).asReadOnlyCopy();
         """.stripMargin,
      forceInline = true)
    val keyEval = packedProbeKey.genCode(ctx)
    ev.copy(code = code"""
      ${keyEval.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.value} = false;
      if (!${keyEval.isNull}) {
        ${ev.value} = $relationTerm.getValue(${keyEval.value}) != null;
      }""",
      isNull = FalseLiteral)
  }

  override lazy val canonicalized: HashedRelationContainsExec = copy(
    packedProbeKey = packedProbeKey.canonicalized,
    plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
    exprId = ExprId(0),
    broadcast = null)

  override protected def withNewChildInternal(newChild: Expression): HashedRelationContainsExec =
    copy(packedProbeKey = newChild)
}
