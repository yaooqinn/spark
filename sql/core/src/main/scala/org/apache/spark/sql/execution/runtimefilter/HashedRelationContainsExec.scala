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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * The physical probe-side predicate that implements HRC.
 *
 * Logically: `bc.value.contains(packedProbeKey.eval(row))`. The `packedProbeKey` must
 * be bound to a Long when the build/probe key is rewritable into a single Long
 * (per `HashJoin.rewriteKeyExpr`); otherwise it falls back to an UnsafeRow encoding.
 * That packing must match exactly what the sibling BroadcastHashJoinExec uses on
 * its build side — they share the same HashedRelation, so they must hash identically.
 *
 * This slice ships the scaffold only — codegen + interpreted-eval body land alongside
 * the physical rewrite in the next sub-batch. The constructor signature is locked here
 * so other scaffolding (BroadcastedHashedRelationRef-aware planning) can compile.
 */
case class HashedRelationContainsExec(
    packedProbeKey: Expression,
    @transient ref: BroadcastedHashedRelationRef)
  extends UnaryExpression with Predicate {

  override def child: Expression = packedProbeKey

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException(
      "HashedRelationContainsExec.eval is a scaffold; actual lookup lands in the " +
        "next implementation slice.")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(
      "HashedRelationContainsExec.doGenCode is a scaffold; actual codegen lands in " +
        "the next implementation slice.")

  override protected def withNewChildInternal(newChild: Expression): HashedRelationContainsExec =
    copy(packedProbeKey = newChild)
}
