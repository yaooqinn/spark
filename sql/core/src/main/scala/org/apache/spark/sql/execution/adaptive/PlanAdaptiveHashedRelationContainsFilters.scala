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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, BindReferences, BloomFilterMightContain, DynamicPruningExpression, Expression, Literal, NamedExpression, XxHash64}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
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
          packedProbeKeys,
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
          HashedRelationContainsExec(packedProbeKeys, ref, NamedExpression.newExprId)
        } else {
          // No reusable sibling BHJ broadcast: drop the filter. BHJ runs
          // unchanged. We intentionally do NOT plan a second
          // BroadcastExchangeExec here (would defeat HRC's raison d'etre).
          Literal.TrueLiteral
        }
    }
  }
}

/**
 * Companion holding SparkPlan-typed BloomFilter helpers ported from
 * `org.apache.spark.sql.catalyst.optimizer.InjectRuntimeFilter` for use by
 * the AQE-only HRC rule body rewrite (E2). Helpers are `private[adaptive]`
 * for direct access from the rule, exposed via `private[sql]` object so the
 * unit suite (same package) can call them.
 *
 * Shape parity with the LogicalPlan versions is intentional. Node-type
 * substitutions:
 *   - Project / Filter (logical) -> ProjectExec / FilterExec
 *   - LogicalRelation / leaf -> LeafExecNode (uses logicalLink.get.stats)
 *   - DynamicPruningSubquery -> DynamicPruningExpression
 *
 * Join transitive-key recursion from the original (ExtractEquiJoinKeys
 * branch) is NOT ported in E1: at AQE post-stage time, join children are
 * typically `QueryStageExec` (leaf-like for scan walks). E2 revisits if a
 * benchmark demands cross-join key chasing.
 */
private[sql] object PlanAdaptiveHashedRelationContainsFilters
    extends JoinSelectionHelper with PredicateHelper {

  private def isSimpleExpression(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY,
      REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE)
  }

  /**
   * SparkPlan-typed port of `InjectRuntimeFilter.extractSelectiveFilterOverScan`.
   * Walks the SparkPlan down through ProjectExec/FilterExec looking for a
   * selective filter sitting above a leaf scan. Returns the (rewritten key,
   * leaf plan) pair on success.
   */
  private[adaptive] def extractSelectiveFilterOverScanExec(
      plan: SparkPlan,
      filterCreationSideKey: Expression): Option[(Expression, SparkPlan)] = {
    def extract(
        p: SparkPlan,
        predicateReference: AttributeSet,
        hasHitFilter: Boolean,
        hasHitSelectiveFilter: Boolean,
        currentPlan: SparkPlan,
        targetKey: Expression): Option[(Expression, SparkPlan)] = p match {
      case ProjectExec(projectList, child) if hasHitFilter =>
        val referencedExprs = projectList.filter(predicateReference.contains)
        if (referencedExprs.forall(isSimpleExpression)) {
          extract(
            child,
            referencedExprs.map(_.references).foldLeft(AttributeSet.empty)(_ ++ _),
            hasHitFilter,
            hasHitSelectiveFilter,
            currentPlan,
            targetKey)
        } else {
          None
        }
      case ProjectExec(_, child) =>
        assert(predicateReference.isEmpty && !hasHitSelectiveFilter)
        extract(child, predicateReference, hasHitFilter, hasHitSelectiveFilter, currentPlan,
          targetKey)
      case FilterExec(condition, child) if isSimpleExpression(condition) =>
        extract(
          child,
          predicateReference ++ condition.references,
          hasHitFilter = true,
          hasHitSelectiveFilter = hasHitSelectiveFilter || isLikelySelective(condition),
          currentPlan,
          targetKey)
      case wsc: WholeStageCodegenExec =>
        extract(wsc.child, predicateReference, hasHitFilter, hasHitSelectiveFilter,
          currentPlan, targetKey)
      case _: InputAdapter =>
        extract(p.children.head, predicateReference, hasHitFilter, hasHitSelectiveFilter,
          currentPlan, targetKey)
      case leaf: LeafExecNode if hasHitSelectiveFilter =>
        Some((targetKey, currentPlan))
      case _ => None
    }
    extract(plan, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
      currentPlan = plan, targetKey = filterCreationSideKey)
  }

  // Returns max scan size in bytes within the subtree, mirroring
  // InjectRuntimeFilter.maxScanByteSize on SparkPlan leaves via logicalLink.
  private def maxScanByteSizeExec(filterApplicationSide: SparkPlan): BigInt = {
    val defaultSizeInBytes = SQLConf.get.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    val leaves = filterApplicationSide.collect { case leaf: LeafExecNode => leaf }
    if (leaves.isEmpty) {
      BigInt(0)
    } else {
      leaves.map { leaf =>
        val sz = leaf.logicalLink.map(_.stats.sizeInBytes).getOrElse(BigInt(defaultSizeInBytes))
        if (sz == defaultSizeInBytes) BigInt(0) else sz
      }.max
    }
  }

  /**
   * SparkPlan-typed port of `InjectRuntimeFilter.satisfyByteSizeRequirement`.
   * True iff max leaf scan byte size meets the BF application-side threshold.
   */
  private[adaptive] def satisfyByteSizeRequirementExec(
      filterApplicationSide: SparkPlan): Boolean = {
    val maxScanSize = maxScanByteSizeExec(filterApplicationSide)
    maxScanSize >=
      SQLConf.get.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
  }

  /**
   * SparkPlan-typed port of `InjectRuntimeFilter.hasDynamicPruningSubquery`.
   * True iff any FilterExec on either subtree references the matching key
   * via a `DynamicPruningExpression`.
   */
  @tailrec
  private[adaptive] def hasDynamicPruningSubqueryExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    def dppKeyOnTop(p: SparkPlan): Option[(Expression, SparkPlan)] = p match {
      case FilterExec(cond, child) =>
        splitConjunctivePredicates(cond).collectFirst {
          case DynamicPruningExpression(_) => (cond, child)
        }
      case _ => None
    }
    (dppKeyOnTop(left), dppKeyOnTop(right)) match {
      case (Some((cond, child)), _) if cond.references.exists(_.fastEquals(leftKey)) ||
          cond.fastEquals(leftKey) =>
        true
      case (Some((_, child)), _) =>
        hasDynamicPruningSubqueryExec(child, right, leftKey, rightKey)
      case (_, Some((cond, child))) if cond.references.exists(_.fastEquals(rightKey)) ||
          cond.fastEquals(rightKey) =>
        true
      case (_, Some((_, child))) =>
        hasDynamicPruningSubqueryExec(left, child, leftKey, rightKey)
      case _ => false
    }
  }

  /**
   * SparkPlan-typed port of `InjectRuntimeFilter.hasBloomFilter`. True iff a
   * `BloomFilterMightContain` keyed on `XxHash64(Seq(key))` already sits in
   * any FilterExec in the subtree.
   */
  private[adaptive] def hasBloomFilterExec(plan: SparkPlan, key: Expression): Boolean = {
    plan.exists {
      case FilterExec(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case BloomFilterMightContain(_, XxHash64(Seq(valueExpression), _))
              if valueExpression.fastEquals(key) => true
          case _ => false
        }
      case _ => false
    }
  }
}
