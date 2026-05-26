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

import org.apache.spark.sql.catalyst.expressions.{BindReferences, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.execution.runtimefilter.{BroadcastedHashedRelationRef, HashedRelationContainsExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * AQE counterpart to [[org.apache.spark.sql.execution.runtimefilter.PlanHashedRelationContainsFilters]].
 * Runs as a queryStageOptimizerRule (post BHJ build stage materialization) and
 * rewrites the [[SubqueryAdaptiveHashedRelationContainsExec]] placeholder
 * planted earlier by [[PlanAdaptiveSubqueries]] into a real
 * [[HashedRelationContainsExec]] wrapping a [[BroadcastedHashedRelationRef]]
 * whose child is the materialized AQE broadcast subtree.
 *
 * Mirrors [[PlanAdaptiveDynamicPruningFilters]] but without the aggregate-fallback
 * branch: HRC has no driver-collect path. If no reusable sibling BHJ broadcast
 * can be found, the rule replaces the filter with Literal.TrueLiteral -- HRC
 * without reuse is a net loss (a second broadcast hashing pass), which is
 * exactly the M1 InSubquery shape M2 was built to retract.
 *
 * See todos features/spark-hashed-relation-contains/docs/0005-investigation-p2b-aqe-audit.md
 * for the 4-axis peer-impl audit that drove this rule.
 */
case class PlanAdaptiveHashedRelationContainsFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  override def conf: SQLConf = rootPlan.context.session.sessionState.conf

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterHashedRelationContainsEnabled) {
      return plan
    }

    plan.transformAllExpressions {
      case HashedRelationContainsExec(
          packedProbeKey,
          SubqueryAdaptiveHashedRelationContainsExec(name, buildKeys, broadcastKeyIndices,
            _, adaptivePlan: AdaptiveSparkPlanExec),
          exprId,
          _) =>
        val packedBuildKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(broadcastKeyIndices.map(buildKeys(_))),
          adaptivePlan.executedPlan.output)
        val mode = HashedRelationBroadcastMode(packedBuildKeys)
        // Plan our own BroadcastExchangeExec; ReuseAdaptiveSubquery (next rule
        // in queryStageOptimizerRules) collapses it against the sibling BHJ
        // broadcast when sameResult holds.
        val exchange = BroadcastExchangeExec(mode, adaptivePlan.executedPlan)

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          find(rootPlan) {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _, _) =>
              left.sameResult(exchange)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _, _) =>
              right.sameResult(exchange)
            case _ => false
          }.isDefined

        if (canReuseExchange) {
          exchange.setLogicalLink(adaptivePlan.executedPlan.logicalLink.get)
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = exchange)
          val ref = BroadcastedHashedRelationRef(newAdaptivePlan)
          HashedRelationContainsExec(packedProbeKey, ref, NamedExpression.newExprId)
        } else {
          // No reusable sibling BHJ broadcast: drop the filter. BHJ runs
          // unchanged. We intentionally do NOT plan a second
          // BroadcastExchangeExec here (would defeat HRC's raison d'etre).
          Literal.TrueLiteral
        }
    }
  }
}
