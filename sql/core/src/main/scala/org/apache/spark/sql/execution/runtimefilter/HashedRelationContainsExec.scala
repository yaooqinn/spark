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
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, Predicate, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral, GenerateUnsafeProjection, JavaCode, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
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
 * Probe-key shape: `packedProbeKeys` is the output of HashJoin.rewriteKeyExpr
 * applied to the logical probe-side keys. It collapses to a single LongType
 * expression for the packed-Long fast path (sum of IntegralType key widths
 * <= 8B), or stays as the original Seq for the UnsafeRow fallback path.
 * The build side is packed via the SAME helper at the SAME call site in
 * PlanHashedRelationContainsFilters / PlanAdaptiveHashedRelationContainsFilters
 * so build/probe shapes are byte-for-byte aligned (matches BHJ's own
 * build/probe use of rewriteKeyExpr at HashJoin.scala line 133 / 136).
 *
 * Codegen path mirrors BHJ.genStreamSideJoinKey (HashJoin.scala line 381-394):
 *  - packed-Long path (length == 1, head.dataType == LongType): emit
 *    `relation.getValue(longKey) != null` inline.
 *  - UnsafeRow fallback (else): emit GenerateUnsafeProjection.createCode +
 *    `relation.getValue(unsafeRow) != null`.
 * The interpreted eval() path mirrors the same two-branch structure for the
 * non-codegen fallback (whole-stage off, debug, certain operators).
 *
 * See:
 *  - features/spark-hashed-relation-contains/docs/0007-investigation-p2c-1-composite-key-design.md
 *    (Open Q1 CLOSED rev 2: BHJ two-path grep verified)
 *  - HashedRelation.scala trait line 43-72 (getValue(Long) + getValue(InternalRow) dual)
 */
case class HashedRelationContainsExec(
    packedProbeKeys: Seq[Expression],
    plan: BaseSubqueryExec,
    exprId: ExprId,
    var broadcast: Broadcast[HashedRelation] = null)
  extends ExecSubqueryExpression
  with Predicate {

  override def children: Seq[Expression] = packedProbeKeys

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  private lazy val isPackedLong: Boolean =
    packedProbeKeys.length == 1 && packedProbeKeys.head.dataType == LongType

  // Lazy UnsafeProjection for the eval()-side UnsafeRow fallback path. Codegen
  // path uses GenerateUnsafeProjection.createCode (compiled into doGenCode body)
  // and bypasses this projection entirely.
  @transient private lazy val unsafeRowProjection: UnsafeProjection =
    UnsafeProjection.create(packedProbeKeys)

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
    val relation = broadcast.value
    if (isPackedLong) {
      val key = packedProbeKeys.head.eval(input)
      if (key == null) false else relation.get(key.asInstanceOf[Long]) != null
    } else {
      val unsafeRow = unsafeRowProjection(input)
      if (unsafeRow.anyNull()) false else relation.get(unsafeRow) != null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (broadcast == null) {
      // Defensive: updateResult should have populated this before codegen.
      // Mirror BloomFilterMightContain behaviour on a null filter.
      return ev.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
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
    if (isPackedLong) {
      // Packed-Long fast path. Mirrors BHJ genStreamSideJoinKey case
      // streamedBoundKeys.length == 1 && head.dataType == LongType.
      val keyEval = packedProbeKeys.head.genCode(ctx)
      ev.copy(code = code"""
        ${keyEval.code}
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = false;
        if (!${keyEval.isNull}) {
          ${ev.value} = $relationTerm.getValue(${keyEval.value}) != null;
        }""",
        isNull = FalseLiteral)
    } else {
      // UnsafeRow fallback path. Mirrors BHJ else-branch which emits
      // GenerateUnsafeProjection.createCode(streamedBoundKeys); the
      // resulting UnsafeRow term is fed straight into
      // HashedRelation.getValue(InternalRow) (UnsafeHashedRelation
      // line 231-264 lookup via BytesToBytesMap).
      val keyEv = GenerateUnsafeProjection.createCode(ctx, packedProbeKeys)
      ev.copy(code = code"""
        ${keyEv.code}
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = false;
        if (!${keyEv.value}.anyNull()) {
          ${ev.value} = $relationTerm.getValue(${keyEv.value}) != null;
        }""",
        isNull = FalseLiteral)
    }
  }

  override lazy val canonicalized: HashedRelationContainsExec = copy(
    packedProbeKeys = packedProbeKeys.map(_.canonicalized),
    plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
    exprId = ExprId(0),
    broadcast = null)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): HashedRelationContainsExec =
    copy(packedProbeKeys = newChildren)
}
