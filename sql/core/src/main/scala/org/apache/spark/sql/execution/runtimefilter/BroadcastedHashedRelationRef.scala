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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BaseSubqueryExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.joins.HashedRelation

/**
 * Holds a reference to the HashedRelation broadcast already produced by the sibling
 * BroadcastHashJoinExec. Unlike SubqueryBroadcastExec, this node does NOT collect
 * the broadcast contents back to the driver — the broadcast stays on the executors
 * and is consumed in-place by HashedRelationContainsExec at probe-row evaluation.
 *
 * The exec node deliberately overrides executeCollect to a hard error: any caller
 * that tries to materialize this subquery to driver-side rows has misused the API,
 * which would silently reintroduce the M1 InSubquery-shape regression (see
 * features/spark-hashed-relation-contains/docs/0002-decision.md §M1->M2 pivot).
 *
 * The actual probe-side rewrite (BroadcastExchangeExec wrapping + sameResult reuse
 * with the BHJ broadcast) lands in PlanHashedRelationContainsFilters per the next
 * implementation slice.
 *
 * @param child must be a BroadcastExchangeExec (or an AdaptiveSparkPlan wrapping one)
 *              that produces a HashedRelation broadcast
 */
case class BroadcastedHashedRelationRef(child: SparkPlan)
  extends BaseSubqueryExec with UnaryExecNode {

  override def name: String = s"hrc-ref#${id}"

  override def output: Seq[Attribute] = child.output

  override def executeCollect(): Array[InternalRow] =
    throw SparkException.internalError(
      "BroadcastedHashedRelationRef must NOT be collected; it is meant to be consumed " +
        "in place by HashedRelationContainsExec via the broadcast() API.")

  override protected def doExecute(): RDD[InternalRow] =
    throw SparkException.internalError(
      "BroadcastedHashedRelationRef.doExecute should never be called.")

  /**
   * The Broadcast[HashedRelation] reference produced (and awaited) by the sibling BHJ.
   *
   * This is the key contract behind HRC: by sharing the same broadcast object across
   * the probe-side filter and the join probe, we avoid a second broadcast materialization
   * and a second hashing pass — exactly what M1's InSubquery shape failed to achieve.
   */
  def broadcast: Broadcast[HashedRelation] = {
    child.executeBroadcast[HashedRelation]()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BroadcastedHashedRelationRef =
    copy(child = newChild)
}
