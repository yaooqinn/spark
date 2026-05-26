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

import org.apache.spark.sql.catalyst.expressions.{BindReferences, HashedRelationContainsSubquery, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.HASHED_RELATION_CONTAINS_SUBQUERY
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.internal.SQLConf

/**
 * Physical preparations rule that rewrites the logical
 * [[org.apache.spark.sql.catalyst.expressions.HashedRelationContainsSubquery]]
 * placeholders inserted by
 * [[org.apache.spark.sql.catalyst.optimizer.InjectHashedRelationFilters]]
 * into [[HashedRelationContainsExec]] wrapping a [[BroadcastedHashedRelationRef]]
 * that reuses the sibling BroadcastHashJoinExec's BroadcastExchangeExec via
 * PlanDynamicPruningFilters reuse pattern; see 0002c-contract.md section 3.5
 * for the full contract).
 *
 * If no sibling BHJ broadcast can be reused, the placeholder is replaced with
 * Literal.TrueLiteral (the filter no-ops; BHJ runs unchanged) -- HRC must never
 * trigger a SECOND broadcast materialization, that would defeat its raison
 * d'etre.
 *
 * AQE is handled by the separate PlanAdaptiveHashedRelationContainsFilters rule
 * (P2b scope) because preparations rules run before InsertAdaptiveSparkPlan
 * wraps the plan as an opaque leaf.
 */
case class PlanHashedRelationContainsFilters(sparkSession: SparkSession)
  extends Rule[SparkPlan] {

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterHashedRelationContainsEnabled) {
      return plan
    }
    plan.transformAllExpressionsWithPruning(
        _.containsPattern(HASHED_RELATION_CONTAINS_SUBQUERY)) {
      case HashedRelationContainsSubquery(
          pruningKey, buildLogicalPlan, buildKeys, broadcastKeyIndices, _, _) =>
        val buildSparkPlan = QueryExecution.createSparkPlan(
          sparkSession.sessionState.planner, buildLogicalPlan)
        // Mirror PlanDynamicPruningFilters: at preparations-time the BHJ build
        // side has not yet been wrapped in BroadcastExchangeExec by
        // EnsureRequirements, so match the unwrapped child and rely on
        // ReuseExchangeAndSubquery (last preparation) to dedup the broadcast
        // exchange we plan here.
        val canReuseExchange = conf.exchangeReuseEnabled && plan.exists {
          case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _, _) =>
            left.sameResult(buildSparkPlan)
          case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _, _) =>
            right.sameResult(buildSparkPlan)
          case _ => false
        }
        if (canReuseExchange) {
          val executedBuild =
            QueryExecution.prepareExecutedPlan(sparkSession, buildSparkPlan)
          val packedBuildKeys = BindReferences.bindReferences(
            HashJoin.rewriteKeyExpr(broadcastKeyIndices.map(buildKeys(_))),
            executedBuild.output)
          val mode = HashedRelationBroadcastMode(packedBuildKeys)
          val exchange = BroadcastExchangeExec(mode, executedBuild)
          val ref = BroadcastedHashedRelationRef(exchange)
          // Single-key path for M2 MVP per 0002c-contract.md section 1;
          // composite-key
          // packing lands in P2c.
          val packedProbeKey = HashJoin.rewriteKeyExpr(Seq(pruningKey)).head
          HashedRelationContainsExec(packedProbeKey, ref, NamedExpression.newExprId)
        } else {
          // No reusable sibling BHJ broadcast: drop the filter. BHJ runs
          // unchanged. We intentionally do NOT plan a second
          // BroadcastExchangeExec here; HRC without reuse is a net loss
          // (broadcast cost without filter benefit).
          Literal.TrueLiteral
        }
    }
  }
}
