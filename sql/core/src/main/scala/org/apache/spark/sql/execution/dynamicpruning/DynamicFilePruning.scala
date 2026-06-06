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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._

/**
 * SPARK-44662 - DynamicFilePruning intentionally mirrors PartitionPruning
 * (sql/core/.../dynamicpruning/PartitionPruning.scala L53-281), diverging at
 * three points documented in design rev 4 (features/spark-dynamic-file-pruning/
 * docs/0002-decision.md):
 *   D1. getFilterableTableScan accepts type-eligible non-partition data cols
 *       (V1 HadoopFsRelation only in v1; Hive + DSv2 deferred)
 *   D2. v1 pruningHasBenefit reuses DPP formula, forces onlyInBroadcast=true
 *       for cost neutrality on non-clustered tables
 *   D3. own conf gate spark.sql.optimizer.dynamicFilePruning.enabled (default
 *       false until smarter hasBenefit heuristic lands in v2)
 *
 * Helper extraction (shared DynamicPruningHelper object) deferred per
 * Rule-of-Three until a third dynamic-pruning sibling appears.
 *
 * Gated by [[org.apache.spark.sql.internal.SQLConf.DYNAMIC_FILE_PRUNING_ENABLED]].
 */
object DynamicFilePruning extends Rule[LogicalPlan]
    with PredicateHelper with JoinSelectionHelper {

  /**
   * Totally-ordered Parquet footer types compatible with JVM Comparable.
   * Mirrors P1b's isEligibleType.
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

  /**
   * D1: accept a V1 HadoopFsRelation scan whose target attribute is a
   * type-eligible non-partition data column. Hive + DSv2 branches deferred.
   */
  private def getFilterableTableScan(a: Expression, plan: LogicalPlan): Option[LogicalPlan] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        l.relation match {
          case _: HadoopFsRelation =>
            if (resExp.references.nonEmpty &&
                resExp.references.forall(r => isEligibleType(r.dataType))) {
              Some(l)
            } else {
              None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.exists {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }
  }

  private def hasPruningFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

  /**
   * D2: v1 forces onlyInBroadcast=true so PlanDynamicPruningFilters emits
   * DPE(TrueLiteral) when no BHJ reuse can be found, which
   * DataSourceScanExec.isDynamicPruningFilter filters out before footer-read
   * (F-S1 invariant test). No subquery duplication ever.
   *
   * isFileFilter=true so InjectRuntimeFilter.hasDynamicPruningSubquery
   * mutex excludes DFP-injected DPS - BF coexists with DFP on the same scan.
   */
  private def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKeys: Seq[Expression],
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression]): LogicalPlan = {
    require(filteringKeys.size == 1)
    val indices = Seq(joinKeys.indexOf(filteringKeys.head))
    Filter(
      DynamicPruningSubquery(
        pruningKey,
        filteringPlan,
        joinKeys,
        indices,
        onlyInBroadcast = true,
        isFileFilter = true),
      pruningPlan)
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // guard against double-injecting on top of DPP or a prior DFP visit
      case j @ Join(Filter(_: DynamicPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: DynamicPruningSubquery, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)
          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
              if fromDifferentSides(a, b) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
                b.references.subsetOf(right.outputSet)) {
              (a, b)
            } else {
              (b, a)
            }
            var filterableScan = getFilterableTableScan(l, left)
            if (filterableScan.isDefined && canPruneLeft(joinType) && hasPruningFilter(right)) {
              newLeft = insertPredicate(l, newLeft, Seq(r), right, rightKeys)
            } else {
              filterableScan = getFilterableTableScan(r, right)
              if (filterableScan.isDefined && canPruneRight(joinType) && hasPruningFilter(left)) {
                newRight = insertPredicate(r, newRight, Seq(l), left, leftKeys)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ if !conf.dynamicFilePruningEnabled => plan
    case _ => prune(plan)
  }
}
