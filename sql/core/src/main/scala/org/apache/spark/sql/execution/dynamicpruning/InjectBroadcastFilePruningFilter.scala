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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference, DynamicPruningExpression, Expression, ExprId, NamedExpression
}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * SPARK-44662 - Dynamic File Pruning for non-partition data columns.
 *
 * This rule is the data-column sibling of [[PlanDynamicPruningFilters]] (non-AQE).
 * When a BroadcastHashJoin's stream side reads from a file-based scan
 * (Parquet V1) on an eligible data column, the rule injects a
 * `DynamicPruningExpression(InSubqueryExec(streamKey, SubqueryBroadcastExec(...)))`
 * into the scan's `dataFilters`. At task-scheduling time the scan's
 * `dynamicallySelectedPartitions` consults the broadcast keys and drops any
 * file whose Parquet footer min/max excludes every build key (in-list bracket).
 *
 * P1b (this batch): rule shell + injection. Footer-level skip happens in
 * P1b-2's extension to `ScanFileListing.filterAndPruneFiles`.
 *
 * Gated by [[SQLConf.DYNAMIC_FILE_PRUNING_ENABLED]] (default false).
 */
case class InjectBroadcastFilePruningFilter(sparkSession: SparkSession)
    extends Rule[SparkPlan] {

  override def conf: SQLConf = sparkSession.sessionState.conf

  /**
   * Eligible data types - pruning column must have totally-ordered footer stats
   * consistent with JVM `Comparable` (design D3 -type system).
   */
  private def isEligibleType(dt: DataType): Boolean = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType => true
    case _: DateType | _: TimestampType => true
    case _: FloatType | _: DoubleType => true
    case _: StringType => true
    case _: BinaryType => true
    case _: DecimalType => true
    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicFilePruningEnabled) {
      return plan
    }
    plan.transformUp {
      case bhj @ BroadcastHashJoinExec(
          leftKeys, rightKeys, _, buildSide, _, left, right, _) =>
        val (streamKeys, buildKeys, streamSide, buildSidePlan) = buildSide match {
          case BuildRight => (leftKeys, rightKeys, left, right)
          case BuildLeft => (rightKeys, leftKeys, right, left)
        }
        if (streamKeys.length != 1 || buildKeys.length != 1) {
          bhj
        } else {
          (streamKeys.head, buildKeys.head) match {
            case (sAttr: AttributeReference, _: NamedExpression)
                if isEligibleType(sAttr.dataType) =>
              tryInject(bhj, sAttr, buildKeys, buildSidePlan, streamSide, buildSide)
            case _ =>
              bhj
          }
        }
    }
  }

  private def tryInject(
      bhj: BroadcastHashJoinExec,
      streamAttr: AttributeReference,
      buildKeys: Seq[Expression],
      buildSidePlan: SparkPlan,
      streamSide: SparkPlan,
      buildSide: BuildSide): SparkPlan = {
    // Find the FileSourceScanExec on the stream side that produces streamAttr.
    val scanOpt = streamSide.collectFirst {
      case s: FileSourceScanExec if s.output.exists(_.exprId == streamAttr.exprId) => s
    }
    scanOpt match {
      case None => bhj
      case Some(scan) =>
        // Locate the broadcast exchange that produced the build side.
        val broadcastExchange = buildSidePlan match {
          case bex: BroadcastExchangeExec => bex
          case other =>
            other.collectFirst { case bex: BroadcastExchangeExec => bex }.orNull
        }
        if (broadcastExchange == null) {
          bhj
        } else {
          val name = s"dfp#${scan.output.find(_.exprId == streamAttr.exprId).get.exprId.id}"
          val sbe = SubqueryBroadcastExec(name, Seq(0), buildKeys, broadcastExchange)
          val inSub = InSubqueryExec(streamAttr, sbe, ExprId(scan.output.find(
            _.exprId == streamAttr.exprId).get.exprId.id))
          val dpe = DynamicPruningExpression(inSub)
          val newScan = scan.copy(dataFilters = scan.dataFilters :+ dpe)
          val newStreamSide = streamSide.transformUp {
            case s: FileSourceScanExec if s eq scan => newScan
          }
          buildSide match {
            case BuildRight => bhj.copy(left = newStreamSide)
            case BuildLeft => bhj.copy(right = newStreamSide)
          }
        }
    }
  }
}
