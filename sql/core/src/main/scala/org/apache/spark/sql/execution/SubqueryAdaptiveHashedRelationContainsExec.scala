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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * AQE-side intermediate physical placeholder for
 * [[org.apache.spark.sql.catalyst.expressions.HashedRelationContainsSubquery]].
 * Mirrors [[SubqueryAdaptiveBroadcastExec]] (the DPP equivalent) -- created by
 * [[org.apache.spark.sql.execution.adaptive.PlanAdaptiveSubqueries]] during AQE
 * preprocessing, then rewritten by
 * [[org.apache.spark.sql.execution.adaptive.PlanAdaptiveHashedRelationContainsFilters]]
 * after the BHJ build stage materializes -- at which point we can plan the real
 * BroadcastExchangeExec wrapped in BroadcastedHashedRelationRef and rely on
 * ReuseAdaptiveSubquery to share the broadcast with the sibling BHJ.
 *
 * This node is intentionally NOT executable: any caller that tries to execute
 * it has misused the API. See 0005-investigation-p2b-aqe-audit.md for the
 * 4-axis peer-impl audit that drove this design.
 *
 * @param buildKeys           build-side join keys; parallel to broadcastKeyIndices
 * @param broadcastKeyIndices indices of the filtering keys collected from the broadcast
 * @param buildPlan           driver-only logical plan (kept transient, used only for trace)
 * @param child               the AdaptiveSparkPlanExec wrapping the build subtree
 */
case class SubqueryAdaptiveHashedRelationContainsExec(
    name: String,
    buildKeys: Seq[Expression],
    broadcastKeyIndices: Seq[Int],
    @transient buildPlan: LogicalPlan,
    child: SparkPlan) extends BaseSubqueryExec with UnaryExecNode {

  protected override def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError(
      "SubqueryAdaptiveHashedRelationContainsExec")
  }

  protected override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    copy(name = "hrc", buildKeys = keys, child = child.canonicalized)
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): SubqueryAdaptiveHashedRelationContainsExec =
    copy(child = newChild)
}
