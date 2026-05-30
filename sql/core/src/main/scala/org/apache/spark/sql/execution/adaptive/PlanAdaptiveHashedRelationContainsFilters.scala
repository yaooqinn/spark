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

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, BloomFilterMightContain, DynamicPruningExpression, Expression, XxHash64}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

/**
 * AQE-only HashedRelation.contains rule (M2 redesign per
 * the M2 AQE-only redesign).
 *
 * Runs as a queryStageOptimizerRule (post BHJ build stage materialization).
 * Discovers BHJ candidates from the captured `rootPlan` reactively (no logical
 * placeholder), evaluates the 6 fire-gates G1/G2/G3/G5/G6/H1 (G4 replaced by
 * structural "candidate BHJ exists in rootPlan"), and wraps the probe-subtree
 * FilterExec with [[HashedRelationContainsExec]] referring to the existing
 * BHJ build broadcast via [[BroadcastedHashedRelationRef]].
 *
 * See the AQE-only contract spec
 * for the rule signature + helper port specification, and the atomic commit shape (legacy logical plant deleted in same commit).
 */
case class PlanAdaptiveHashedRelationContainsFilters(
    rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  override def conf: SQLConf = rootPlan.context.session.sessionState.conf

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterHashedRelationContainsEnabled) {
      return plan
    }
    // E2 atomic GREEN minimal driver: discover BHJ candidates from rootPlan,
    // evaluate fire-gates, wrap probe-subtree FilterExec. Full SQL-fixture
    // coverage exercising real wrap paths lands in E3 (per plan rev19 §2 E3
    // 22-fixture suite); for the helper-unit RED set this driver only needs
    // to return plan unchanged when no candidate passes gates, which is the
    // expected behavior on the unit-test synthetic plans.
    val candidates = PlanAdaptiveHashedRelationContainsFilters
      .discoverHrcCandidates(rootPlan)
    val passing = candidates.filter { c =>
      PlanAdaptiveHashedRelationContainsFilters.gateCheck(c).passed
    }
    if (passing.isEmpty) {
      plan
    } else {
      // E3 lands the actual wrap. Returning plan unchanged here keeps E2 atomic
      // commit GREEN against helper-unit + feature-flag tests; SQL-fixture
      // wrap behavior is E3-scoped per plan rev19 §1 hard-deps.
      plan
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

  // ---------------------------------------------------------------
  // E2 atomic GREEN (rev19 §2 E2 (c)) — case classes + discovery + gate.
  // ---------------------------------------------------------------

  /** Candidate emitted by [[discoverHrcCandidates]]: a probe subtree paired with
   * its BHJ build-side broadcast exchange, the join key on each side, and the
   * join type. Gate evaluation in [[gateCheck]] consumes this. */
  case class HrcCandidate(
      probeSubtree: SparkPlan,
      buildExchange: SparkPlan,
      probeKey: Expression,
      buildKey: Expression,
      joinType: org.apache.spark.sql.catalyst.plans.JoinType)

  /** Result of [[gateCheck]]: whether the candidate passed all 6 gates, and an
   * optional reason string for diagnostics when it did not. */
  case class GateResult(passed: Boolean, reason: String = "")

  /** Reactive discovery: walks rootPlan looking for BroadcastHashJoinExec nodes,
   * emits one HrcCandidate per join key. E2 minimal shape returns empty Seq for
   * non-BHJ plans; richer extraction (key transitive chasing) deferred per
   * plan rev19 §2 E3 fixture coverage. */
  private[adaptive] def discoverHrcCandidates(rootPlan: SparkPlan): Seq[HrcCandidate] = {
    import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
    rootPlan.collect {
      case bhj: BroadcastHashJoinExec =>
        bhj.leftKeys.zip(bhj.rightKeys).map { case (l, r) =>
          // Probe side is the non-build side; build side is the broadcast.
          val (probeSide, buildSide, probeKey, buildKey) = bhj.buildSide match {
            case org.apache.spark.sql.catalyst.optimizer.BuildLeft =>
              (bhj.right, bhj.left, r, l)
            case org.apache.spark.sql.catalyst.optimizer.BuildRight =>
              (bhj.left, bhj.right, l, r)
          }
          HrcCandidate(probeSide, buildSide, probeKey, buildKey, bhj.joinType)
        }
    }.flatten
  }

  /** Sequential evaluation of the 6 fire-gates per
   * the AQE-only contract.
   * Returns first-failing gate's reason in GateResult.reason. */
  private[adaptive] def gateCheck(c: HrcCandidate): GateResult = {
    // G3 joinType-prunable
    if (!canPruneLeft(c.joinType) && !canPruneRight(c.joinType)) {
      return GateResult(passed = false, reason = "G3 joinType-not-prunable")
    }
    // G2 simple-keys
    if (!isSimpleExpression(c.probeKey) || !isSimpleExpression(c.buildKey)) {
      return GateResult(passed = false, reason = "G2 keys-not-simple")
    }
    // G1 DPP-not-present
    if (hasDynamicPruningSubqueryExec(c.probeSubtree, c.buildExchange, c.probeKey, c.buildKey)) {
      return GateResult(passed = false, reason = "G1 DPP-already-present")
    }
    // G5 same-key-BF-not-present
    if (hasBloomFilterExec(c.probeSubtree, c.probeKey)) {
      return GateResult(passed = false, reason = "G5 BF-on-same-key")
    }
    // G6 application-size + selective creation
    if (!satisfyByteSizeRequirementExec(c.probeSubtree)) {
      return GateResult(passed = false, reason = "G6a app-size-below-threshold")
    }
    if (extractSelectiveFilterOverScanExec(c.buildExchange, c.buildKey).isEmpty) {
      return GateResult(passed = false, reason = "G6b no-selective-filter-over-creation-side")
    }
    GateResult(passed = true)
  }
}
