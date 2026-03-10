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

package org.apache.spark.sql.catalyst.optimizer

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY, PYTHON_UDF, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, SCALA_UDF}
import org.apache.spark.sql.internal.SQLConf

/**
 * Insert a runtime filter on one side of the join (we call this side the application side) if
 * we can extract a runtime filter from the other side (creation side). A simple case is that
 * the creation side is a table scan with a selective filter.
 * The runtime filter is logically an IN subquery with the join keys. Currently it's always
 * bloom filter but we may add other physical implementations in the future.
 */
object InjectRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper with JoinSelectionHelper {

  // Inject a composite bloom filter using all provided join keys for better selectivity.
  // When a join has multiple equi-join keys (e.g., ON a.k1 = b.k1 AND a.k2 = b.k2),
  // hashing all keys together produces a much more selective bloom filter than
  // hashing each key individually.
  private def injectFilter(
      filterApplicationSideKeys: Seq[Expression],
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideKeys: Seq[Expression],
      filterCreationSidePlan: LogicalPlan): LogicalPlan = {
    injectBloomFilter(
      filterApplicationSideKeys,
      filterApplicationSidePlan,
      filterCreationSideKeys,
      filterCreationSidePlan
    )
  }

  private def injectBloomFilter(
      filterApplicationSideKeys: Seq[Expression],
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideKeys: Seq[Expression],
      filterCreationSidePlan: LogicalPlan): LogicalPlan = {
    require(filterApplicationSideKeys.nonEmpty)
    require(filterApplicationSideKeys.length == filterCreationSideKeys.length)
    // Skip if the filter creation side is too big
    if (filterCreationSidePlan.stats.sizeInBytes > conf.runtimeFilterCreationSideThreshold) {
      return filterApplicationSidePlan
    }
    val rowCount = filterCreationSidePlan.stats.rowCount
    // Hash all creation-side keys together for a composite bloom filter
    val bloomFilterAgg =
      if (rowCount.isDefined && rowCount.get.longValue > 0L) {
        new BloomFilterAggregate(
          new XxHash64(filterCreationSideKeys), rowCount.get.longValue)
      } else {
        new BloomFilterAggregate(new XxHash64(filterCreationSideKeys))
      }

    val alias = Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()
    val aggregate =
      ConstantFolding(ColumnPruning(Aggregate(Nil, Seq(alias), filterCreationSidePlan)))
    val bloomFilterSubquery = ScalarSubquery(aggregate, Nil)
    // Hash all application-side keys together to match
    val filter = BloomFilterMightContain(bloomFilterSubquery,
      new XxHash64(filterApplicationSideKeys))
    Filter(filter, filterApplicationSidePlan)
  }

  /**
   * Extracts a sub-plan which is a simple filter over scan from the input plan. The simple
   * filter should be selective and the filter condition (including expressions in the child
   * plan referenced by the filter condition) should be a simple expression, so that we do
   * not add a subquery that might have an expensive computation. The extracted sub-plan should
   * produce a superset of the entire creation side output data, so that it's still correct to
   * use the sub-plan to build the runtime filter to prune the application side.
   */
  private def extractSelectiveFilterOverScan(
      plan: LogicalPlan,
      filterCreationSideKey: Expression): Option[(Expression, LogicalPlan)] = {
    def extract(
        p: LogicalPlan,
        predicateReference: AttributeSet,
        hasHitFilter: Boolean,
        hasHitSelectiveFilter: Boolean,
        currentPlan: LogicalPlan,
        targetKey: Expression): Option[(Expression, LogicalPlan)] = p match {
      case Project(projectList, child) if hasHitFilter =>
        // We need to make sure all expressions referenced by filter predicates are simple
        // expressions.
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
      case Project(_, child) =>
        assert(predicateReference.isEmpty && !hasHitSelectiveFilter)
        extract(child, predicateReference, hasHitFilter, hasHitSelectiveFilter, currentPlan,
          targetKey)
      case Filter(condition, child) if isSimpleExpression(condition) =>
        extract(
          child,
          predicateReference ++ condition.references,
          hasHitFilter = true,
          hasHitSelectiveFilter = hasHitSelectiveFilter || isLikelySelective(condition),
          currentPlan,
          targetKey)
      case ExtractEquiJoinKeys(joinType, lkeys, rkeys, _, _, left, right, _) =>
        // Runtime filters use one side of the [[Join]] to build a set of join key values and prune
        // the other side of the [[Join]]. It's also OK to use a superset of the join key values
        // (ignore null values) to do the pruning. We can also extract from the other side if the
        // join keys are transitive, and the other side always produces a superset output of join
        // key values. Any join side always produce a superset output of its corresponding
        // join keys, but for transitive join keys we need to check the join type.
        // We assume other rules have already pushed predicates through join if possible.
        // So the predicate references won't pass on anymore.
        if (left.output.exists(_.semanticEquals(targetKey))) {
          extract(left, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
            currentPlan = left, targetKey = targetKey).orElse {
            // An example that extract from the right side if the join keys are transitive.
            //     left table: 1, 2, 3
            //     right table, 3, 4
            //     right outer join output: (3, 3), (null, 4)
            //     right key output: 3, 4
            if (canPruneLeft(joinType)) {
              lkeys.zip(rkeys).find(_._1.semanticEquals(targetKey)).map(_._2)
                .flatMap { newTargetKey =>
                  extract(right, AttributeSet.empty,
                    hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = right,
                    targetKey = newTargetKey)
                }
            } else {
              None
            }
          }
        } else if (right.output.exists(_.semanticEquals(targetKey))) {
          extract(right, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
            currentPlan = right, targetKey = targetKey).orElse {
            // An example that extract from the left side if the join keys are transitive.
            // left table: 1, 2, 3
            // right table, 3, 4
            // left outer join output: (1, null), (2, null), (3, 3)
            // left key output: 1, 2, 3
            if (canPruneRight(joinType)) {
              rkeys.zip(lkeys).find(_._1.semanticEquals(targetKey)).map(_._2)
                .flatMap { newTargetKey =>
                  extract(left, AttributeSet.empty,
                    hasHitFilter = false, hasHitSelectiveFilter = false, currentPlan = left,
                    targetKey = newTargetKey)
                }
            } else {
              None
            }
          }
        } else {
          None
        }
      case _: LeafNode if hasHitSelectiveFilter =>
        Some((targetKey, currentPlan))
      case _ => None
    }

    if (!plan.isStreaming) {
      extract(plan, AttributeSet.empty, hasHitFilter = false, hasHitSelectiveFilter = false,
        currentPlan = plan, targetKey = filterCreationSideKey)
    } else {
      None
    }
  }

  private def isSimpleExpression(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY,
      REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE)
  }

  private def isProbablyShuffleJoin(left: LogicalPlan,
      right: LogicalPlan, hint: JoinHint): Boolean = {
    !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) &&
      !canBroadcastBySize(left, conf) && !canBroadcastBySize(right, conf)
  }

  private def probablyHasShuffle(plan: LogicalPlan): Boolean = {
    plan.exists {
      case Join(left, right, _, _, hint) => isProbablyShuffleJoin(left, right, hint)
      case _: Aggregate => true
      case _: Window => true
      case _ => false
    }
  }

  // Returns the max scan byte size in the subtree rooted at `filterApplicationSide`.
  private def maxScanByteSize(filterApplicationSide: LogicalPlan): BigInt = {
    val defaultSizeInBytes = conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    filterApplicationSide.collect({
      case leaf: LeafNode => leaf
    }).map(scan => {
      // DEFAULT_SIZE_IN_BYTES means there's no byte size information in stats. Since we avoid
      // creating a Bloom filter when the filter application side is very small, so using 0
      // as the byte size when the actual size is unknown can avoid regression by applying BF
      // on a small table.
      if (scan.stats.sizeInBytes == defaultSizeInBytes) BigInt(0) else scan.stats.sizeInBytes
    }).max
  }

  // Returns true if `filterApplicationSide` satisfies the byte size requirement to apply a
  // Bloom filter; false otherwise.
  private def satisfyByteSizeRequirement(filterApplicationSide: LogicalPlan): Boolean = {
    // In case `filterApplicationSide` is a union of many small tables, disseminating the Bloom
    // filter to each small task might be more costly than scanning them itself. Thus, we use max
    // rather than sum here.
    val maxScanSize = maxScanByteSize(filterApplicationSide)
    maxScanSize >=
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
  }

  /**
   * Extracts the beneficial filter creation plan with check show below:
   * - The filterApplicationSideKey can be pushed down through joins, aggregates and windows
   *   (ie the expression references originate from a single leaf node)
   * - The filter creation side has a selective predicate
   * - The max filterApplicationSide scan size is greater than a configurable threshold
   */
  private def extractBeneficialFilterCreatePlan(
      filterApplicationSide: LogicalPlan,
      filterCreationSide: LogicalPlan,
      filterApplicationSideKey: Expression,
      filterCreationSideKey: Expression): Option[(Expression, LogicalPlan)] = {
    if (findExpressionAndTrackLineageDown(
      filterApplicationSideKey, filterApplicationSide).isDefined &&
      satisfyByteSizeRequirement(filterApplicationSide)) {
      extractSelectiveFilterOverScan(filterCreationSide, filterCreationSideKey)
    } else {
      None
    }
  }

  // This checks if there is already a DPP filter, as this rule is called just after DPP.
  @tailrec
  private def hasDynamicPruningSubquery(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    (left, right) match {
      case (Filter(DynamicPruningSubquery(pruningKey, _, _, _, _, _, _), plan), _) =>
        pruningKey.fastEquals(leftKey) || hasDynamicPruningSubquery(plan, right, leftKey, rightKey)
      case (_, Filter(DynamicPruningSubquery(pruningKey, _, _, _, _, _, _), plan)) =>
        pruningKey.fastEquals(rightKey) ||
          hasDynamicPruningSubquery(left, plan, leftKey, rightKey)
      case _ => false
    }
  }

  private def hasBloomFilter(plan: LogicalPlan, keys: Seq[Expression]): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case BloomFilterMightContain(_, XxHash64(hashKeys, _))
            if hashKeys.length == keys.length &&
              hashKeys.zip(keys).forall { case (h, k) => h.fastEquals(k) } => true
          case _ => false
        }
      case _ => false
    }
  }

  private def tryInjectRuntimeFilter(plan: LogicalPlan): LogicalPlan = {
    var filterCounter = 0
    val numFilterThreshold = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD)
    plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        // Collect all simple equi-join key pairs that don't already have DPP filters.
        val eligibleKeyPairs = leftKeys.zip(rightKeys).filter { case (l, r) =>
          !hasDynamicPruningSubquery(left, right, l, r) &&
            isSimpleExpression(l) && isSimpleExpression(r)
        }
        if (filterCounter < numFilterThreshold && eligibleKeyPairs.nonEmpty) {
          val hasShuffle = isProbablyShuffleJoin(left, right, hint)
          if (eligibleKeyPairs.length > 1) {
            // Multi-key join: inject ONE composite bloom filter using all keys together
            // for higher selectivity (e.g., hash(ticket_number, item_sk) vs hash(item_sk))
            val (eligibleLeftKeys, eligibleRightKeys) = eligibleKeyPairs.unzip
            val oldLeft = newLeft
            val oldRight = newRight
            if (canPruneLeft(joinType) && (hasShuffle || probablyHasShuffle(left)) &&
              !hasBloomFilter(newLeft, eligibleLeftKeys)) {
              extractBeneficialFilterCreatePlan(
                left, right, eligibleLeftKeys.head, eligibleRightKeys.head).foreach {
                case (_, filterCreationSidePlan) =>
                  val resolvedCreationKeys = eligibleRightKeys.flatMap { rk =>
                    findExpressionAndTrackLineageDown(rk, filterCreationSidePlan).map(_._1)
                  }
                  if (resolvedCreationKeys.length == eligibleRightKeys.length) {
                    newLeft = injectFilter(
                      eligibleLeftKeys, newLeft, resolvedCreationKeys, filterCreationSidePlan)
                  }
              }
            }
            if (newLeft.fastEquals(oldLeft) && canPruneRight(joinType) &&
              (hasShuffle || probablyHasShuffle(right)) &&
              !hasBloomFilter(newRight, eligibleRightKeys)) {
              extractBeneficialFilterCreatePlan(
                right, left, eligibleRightKeys.head, eligibleLeftKeys.head).foreach {
                case (_, filterCreationSidePlan) =>
                  val resolvedCreationKeys = eligibleLeftKeys.flatMap { lk =>
                    findExpressionAndTrackLineageDown(lk, filterCreationSidePlan).map(_._1)
                  }
                  if (resolvedCreationKeys.length == eligibleLeftKeys.length) {
                    newRight = injectFilter(
                      eligibleRightKeys, newRight, resolvedCreationKeys, filterCreationSidePlan)
                  }
              }
            }
            if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
              filterCounter = filterCounter + 1
            }
          } else {
            // Single-key join: use original per-key logic for backward compatibility
            eligibleKeyPairs.foreach { case (l, r) =>
              if (filterCounter < numFilterThreshold) {
                val oldLeft = newLeft
                val oldRight = newRight
                if (canPruneLeft(joinType) && (hasShuffle || probablyHasShuffle(left)) &&
                  !hasBloomFilter(newLeft, Seq(l))) {
                  extractBeneficialFilterCreatePlan(left, right, l, r).foreach {
                    case (filterCreationSideKey, filterCreationSidePlan) =>
                      newLeft = injectFilter(
                        Seq(l), newLeft, Seq(filterCreationSideKey), filterCreationSidePlan)
                  }
                }
                if (newLeft.fastEquals(oldLeft) && canPruneRight(joinType) &&
                  (hasShuffle || probablyHasShuffle(right)) &&
                  !hasBloomFilter(newRight, Seq(r))) {
                  extractBeneficialFilterCreatePlan(right, left, r, l).foreach {
                    case (filterCreationSideKey, filterCreationSidePlan) =>
                      newRight = injectFilter(
                        Seq(r), newRight, Seq(filterCreationSideKey), filterCreationSidePlan)
                  }
                }
                if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
                  filterCounter = filterCounter + 1
                }
              }
            }
          }
        }
        join.withNewChildren(Seq(newLeft, newRight))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ if !conf.runtimeFilterBloomFilterEnabled => plan
    case _ => tryInjectRuntimeFilter(plan)
  }

}
