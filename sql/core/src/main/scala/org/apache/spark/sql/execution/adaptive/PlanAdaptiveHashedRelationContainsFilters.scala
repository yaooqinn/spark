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

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, BloomFilterMightContain, DynamicPruningExpression, Expression, NamedExpression, XxHash64}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin}
import org.apache.spark.sql.execution.runtimefilter.{BroadcastedHashedRelationRef, HashedRelationContainsExec}
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
    plan.transform {
      case bhj: BroadcastHashJoinExec =>
        PlanAdaptiveHashedRelationContainsFilters.maybeWrapStreamedSide(bhj)
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
    // G6 application-size gate (HRC-adapted).
    //
    // In AQE post-stage time the BHJ build broadcast has already passed the
    // autoBroadcastJoinThreshold gate, so the BHJ itself is a structural
    // guarantee of "small build, large probe worth filtering". HRC wrap
    // overhead is one hash-lookup per probe row against an already-materialized
    // HashedRelation (no extra build, unlike Bloom filter), so the BF-derived
    // probe-scan-size threshold (RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD,
    // default 10GB) does not apply to HRC: most TPC-DS BHJ probes have logical
    // sizeInBytes that AQE has rewritten down through prior joins to either
    // small intermediate estimates or the default 1GB sentinel (which the BF
    // helper deliberately treats as 0 -- correct for BF, wrong for HRC).
    //
    // We therefore drop G6a for HRC. The skip mirrors the G6b drop above and
    // is design-correct: if the BHJ exists post-stage, HRC wrap is cheaper than
    // a fresh BF inject, and the gate it would replicate (BHJ broadcast
    // threshold) already fired upstream.
    // G6a (deprecated for HRC, preserved as design comment):
    //   if (!satisfyByteSizeRequirementExec(unwrapQueryStages(c.probeSubtree))) ...
    GateResult(passed = true)
  }

  /** Unwrap QueryStageExec wrappers (BroadcastQueryStageExec /
   * ShuffleQueryStageExec / ResultQueryStageExec) and ReusedExchangeExec to
   * expose the underlying scan tree for byte-size / scan-walk gates. */
  private def unwrapQueryStages(plan: SparkPlan): SparkPlan = plan match {
    case qs: QueryStageExec => unwrapQueryStages(qs.plan)
    case _ => plan
  }

  /**
   * Reactive wrap: given a freshly-encountered BroadcastHashJoinExec whose
   * build side is a materialized BroadcastQueryStageExec (or any
   * BroadcastExchangeLike), evaluate fire-gates and (if all pass) wrap the
   * streamed side with FilterExec(HashedRelationContainsExec). Returns the
   * original BHJ unchanged when any gate fails or wrap is unsafe.
   *
   * The HRC node holds a [[BroadcastedHashedRelationRef]] over the BHJ's own
   * build-side exchange, so the broadcast is shared (no second materialization);
   * this is the AQE-only invariant that motivated the M2 redesign.
   */
  private[adaptive] def maybeWrapStreamedSide(bhj: BroadcastHashJoinExec): SparkPlan = {
    // Single-key shape only in E3: composite key support deferred (per plan
    // rev19 §2 E3 -- multi-key probe-side key packing duplicates the BHJ
    // own packing and is a follow-up batch).
    if (bhj.leftKeys.size != 1 || bhj.rightKeys.size != 1) return bhj
    val (probeSide, buildSide, probeKey, buildKey) = bhj.buildSide match {
      case BuildLeft => (bhj.right, bhj.left, bhj.rightKeys.head, bhj.leftKeys.head)
      case BuildRight => (bhj.left, bhj.right, bhj.leftKeys.head, bhj.rightKeys.head)
    }
    // Idempotence guard: if the streamed side is already wrapped with HRC for
    // this key, do not wrap again. AQE may re-apply queryStageOptimizerRules
    // across stages; this keeps the rule a fixed-point.
    if (alreadyWrappedHrc(probeSide)) return bhj
    val candidate = HrcCandidate(probeSide, buildSide, probeKey, buildKey, bhj.joinType)
    val gate = gateCheck(candidate)
    if (!gate.passed) return bhj

    // The build side at this point in AQE rewrite is either a
    // BroadcastQueryStageExec (post-materialization) or a BroadcastExchangeExec
    // (pre-materialization, on the first apply pass before stage submission).
    // BroadcastedHashedRelationRef accepts either: its .broadcast accessor
    // forwards .executeBroadcast to child, which both shapes support.
    val ref = BroadcastedHashedRelationRef(buildSide)
    val packedProbeKey = HashJoin.rewriteKeyExpr(Seq(probeKey)) match {
      case Seq(packed) => packed
      case other => other.head // multi-key shape unreachable here (single-key gate above)
    }
    val hrc = HashedRelationContainsExec(
      packedProbeKeys = Seq(packedProbeKey),
      plan = ref,
      exprId = NamedExpression.newExprId)
    val wrapped = FilterExec(hrc, probeSide)

    bhj.buildSide match {
      case BuildLeft => bhj.copy(right = wrapped)
      case BuildRight => bhj.copy(left = wrapped)
    }
  }

  private def alreadyWrappedHrc(probe: SparkPlan): Boolean = probe match {
    case FilterExec(cond, _) =>
      splitConjunctivePredicates(cond).exists(_.isInstanceOf[HashedRelationContainsExec])
    case _ => false
  }
}
