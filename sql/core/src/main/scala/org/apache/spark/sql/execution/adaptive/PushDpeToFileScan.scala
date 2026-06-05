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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions.{
  And, DynamicPruningExpression, Expression, PredicateHelper
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  FileSourceScanExec, FilterExec, InSubqueryExec, ProjectExec, SparkPlan, SubqueryBroadcastExec
}
import org.apache.spark.sql.internal.SQLConf

/**
 * SPARK-44662 - Move materialized DynamicPruningExpression from a FilterExec
 * into the downstream FileSourceScanExec.dataFilters at AQE
 * queryStageOptimizerRules timing. Bypasses FileSourceStrategy:194 strip
 * (PLAN_EXPRESSION removed before scan creation; see investigations/0009 +
 * decision rev 7).
 *
 * Dependencies:
 *  - Must run AFTER PlanAdaptiveDynamicPruningFilters: matches materialized
 *    InSubqueryExec(_, _: SubqueryBroadcastExec, ...) shape, not the logical
 *    DynamicPruningSubquery placeholder.
 *  - Must run BEFORE ReuseAdaptiveSubquery: reuse detection walks final plan
 *    and must see the moved DPE.
 *  - Re-fire safety: AdaptiveSparkPlanExec.reOptimize produces fresh scan
 *    instances; QueryStageExec extends LeafExecNode so transformUp treats
 *    materialized stages as opaque leaves. No double-append (decision rev 7
 *    M-D1 GROUNDED-LOW).
 *  - Scan-instance ownership: at queryStageOptimizerRules timing each stage
 *    subtree owns a unique scan instance, not shared with sibling stages
 *    (stage2-r6 F6-H2).
 *
 * ColumnarToRowExec walk arm is defensive-only: ApplyColumnarRulesAndInsertTransitions
 * lives in postStageCreationRules (AdaptiveSparkPlanExec.scala:154-158) and
 * runs AFTER optimizeQueryStage (line 655), so ColumnarToRow is provably
 * absent here at rule timing. Arm kept to future-proof rule-ordering
 * regression (stage2-r7 M-N6).
 */
object PushDpeToFileScan extends Rule[SparkPlan] with PredicateHelper {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.dynamicFilePruningEnabled) {
      return plan
    }
    plan.transformUp {
      case f @ FilterExec(condition, child) =>
        findScanThroughBenignWrappers(child) match {
          case Some(scan) => moveDpesIntoScan(f, condition, scan, child)
          case None => f
        }
    }
  }

  /**
   * Walks `p` through ProjectExec (only intermediate node reachable at this
   * rule timing; deep-projects survive RemoveRedundantProjects in
   * queryStagePreparationRules). ColumnarToRowExec arm is defensive only -
   * not reachable here in current Spark, kept to future-proof rule ordering.
   *
   * Returns Some(scan) iff a chain through ONLY ProjectExec terminates in a
   * FileSourceScanExec, else None.
   */
  private def findScanThroughBenignWrappers(p: SparkPlan): Option[FileSourceScanExec] = p match {
    case s: FileSourceScanExec => Some(s)
    case ProjectExec(_, child) => findScanThroughBenignWrappers(child)
    // Defensive arm - not reachable at queryStageOptimizerRules timing in current Spark.
    case c if c.getClass.getName.endsWith("ColumnarToRowExec") =>
      findScanThroughBenignWrappers(c.children.head)
    case _ => None
  }

  /**
   * Splits Filter condition into (movable DPEs, non-DPE remainder).
   * Movable DPE = DynamicPruningExpression wrapping post-materialization
   * InSubqueryExec(_, _: SubqueryBroadcastExec, ...). DPE(TrueLiteral)
   * fallback is NOT matched - it remains in Filter so isDynamicPruningFilter
   * (DataSourceScanExec) can strip it before footer-read (F-S1 invariant).
   */
  private def isMovableDpe(e: Expression): Boolean = e match {
    case DynamicPruningExpression(
        InSubqueryExec(_, _: SubqueryBroadcastExec, _, _, _, _)) => true
    case _ => false
  }

  private def moveDpesIntoScan(
      filter: FilterExec,
      condition: Expression,
      scan: FileSourceScanExec,
      originalChild: SparkPlan): SparkPlan = {
    val conds = splitConjunctivePredicates(condition)
    val (dpes, rest) = conds.partition(isMovableDpe)
    if (dpes.isEmpty) {
      filter
    } else {
      val newScan = scan.copy(dataFilters = scan.dataFilters ++ dpes)
      val newChild = originalChild.transformDown {
        case s: FileSourceScanExec if s.eq(scan) => newScan
      }
      rest match {
        case Nil => newChild
        case head :: tail =>
          FilterExec(tail.foldLeft(head)(And), newChild)
      }
    }
  }
}
