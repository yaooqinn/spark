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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Cost model gating HashedRelationContains injection. Per-rule-invocation pure
 * decisions + caller-managed per-scan budget Map (filtersInjectedPerScan). All
 * formulas are size-based (4 SQLConf AND-gates, no selectivity); selectivity
 * and cost-vs-benefit ranking are deferred pending benchmark evidence (see JIRA
 * SPARK-XXXXX, sections "Q2-bis SPIP 6.2 deviation" and "Q3-bis SPIP 6.3
 * deviation").
 *
 * Thread-safety: this object holds no mutable state. The budget Map is owned by
 * the caller (`InjectHashedRelationFilters.apply`), constructed fresh per call,
 * not shared across concurrent queries.
 *
 * Budget key anchor (per-scan): `output.head.exprId.id: Long` of the probe-side
 * leaf scan. ExprId is stable across `transformWithPruning` (verified via
 * spike, see JIRA SPARK-XXXXX); peer-parity with `InjectRuntimeFilter`
 * `hasBloomFilter` fastEquals-on-AttributeReference; self-join semantics
 * aligned with the ExprId-strict Bloom mutex.
 */
private[sql] object HashedRelationFilterCostModel {

  sealed trait Decision { def reason: String }
  final case class Inject(reason: String) extends Decision
  final case class Skip(reason: String) extends Decision

  /**
   * D.5 partial wire -- MinApplicationSize + MaxBuildSize + MaxFiltersPerScan
   * + CreationSideThreshold gates. D.6 batch will extend with the Bloom mutex
   * check per JIRA SPARK-XXXXX section 5.
   *
   * Per-scan budget: the caller passes `probeScanAnchor` (the
   * `output.head.exprId.id` of the probe-side leaf scan) and a mutable `budget`
   * Map shared across all `shouldInject` calls within a single rule invocation.
   * When the budget for this anchor has reached `maxFiltersPerScan`, the
   * decision is `Skip("per-scan-budget-exhausted")`. The caller increments the
   * budget after a successful inject (cost-model is read-only on budget).
   *
   * CreationSideThreshold is checked on `buildPlan.stats.sizeInBytes` (bytes),
   * distinct from MaxBuildSize which gates on `stats.rowCount` (rows). The two
   * gates are complementary: rowCount catches narrow-wide-row mismatch (e.g.
   * 100 wide rows < 1M row cap but huge bytes), sizeInBytes catches the
   * physical broadcast cost ceiling regardless of row count.
   */
  def shouldInject(
      buildPlan: LogicalPlan,
      probePlan: LogicalPlan,
      probeScanAnchor: Long,
      budget: mutable.Map[Long, Int],
      conf: SQLConf): Decision = {
    val buildSizeInBytesBig = buildPlan.stats.sizeInBytes
    val buildRowCountOpt = buildPlan.stats.rowCount.map(_.toLong)
    // Fail-open on missing build rowCount (CBO off or no column stats): treat
    // unknown as 0 rows so the MaxBuildSize gate does not silently Skip every
    // probe site in a CBO-off cluster. Pre-D.5 behaviour was fail-closed
    // (.getOrElse(Long.MaxValue)), which produced the brick-wall described in
    // the P2d Stage 5 review F1.6. The downstream canBroadcastBySize gate in
    // the rule still defends against actually-unboundable builds via the
    // broadcast threshold.
    val buildRowCount = buildRowCountOpt.getOrElse(0L)
    // BigInt.toLong is mod 2^64 with no overflow signal; isValidLong guards
    // against silent truncation on pathological multiplicative stats estimates
    // (large fact-table join cardinality x row width).
    val buildSizeInBytes =
      if (buildSizeInBytesBig.isValidLong) buildSizeInBytesBig.toLong else Long.MaxValue
    val probeRowCount = probePlan.stats.rowCount.map(_.toLong).getOrElse(0L)
    val maxBuildRows = conf.runtimeFilterHashedRelationContainsMaxBuildSize
    val creationSideThresholdBytes =
      conf.runtimeFilterHashedRelationContainsCreationSideThreshold
    val minAppRows = conf.runtimeFilterHashedRelationContainsMinApplicationSize
    val maxFiltersPerScan = conf.runtimeFilterHashedRelationContainsMaxFiltersPerScan
    val injectedSoFar = budget.getOrElse(probeScanAnchor, 0)
    if (buildRowCount > maxBuildRows) {
      Skip(s"max-build-rows-exceeded: $buildRowCount > $maxBuildRows")
    } else if (buildSizeInBytes > creationSideThresholdBytes) {
      Skip(s"creation-side-threshold-exceeded: $buildSizeInBytes > " +
        s"$creationSideThresholdBytes bytes")
    } else if (probeRowCount < minAppRows) {
      Skip(s"min-application-rows-not-met: $probeRowCount < $minAppRows")
    } else if (injectedSoFar >= maxFiltersPerScan) {
      Skip(s"per-scan-budget-exhausted: anchor=$probeScanAnchor " +
        s"injected=$injectedSoFar limit=$maxFiltersPerScan")
    } else {
      val statsHint = if (buildRowCountOpt.isEmpty) " (build-stats-unavailable)" else ""
      Inject(s"all-gates-passed: buildRows=$buildRowCount " +
        s"buildBytes=$buildSizeInBytes probeRows=$probeRowCount " +
        s"anchor=$probeScanAnchor injected=$injectedSoFar/$maxFiltersPerScan" +
        statsHint)
    }
  }

  /**
   * Rank candidate builds by size ascending (smaller broadcast = cheaper). Used
   * when a single probe scan has multiple candidate HRC injections competing
   * for the `maxFiltersPerScan` budget.
   *
   * Note: SPIP section 6.2 proposes a selectivity formula
   * `1 - buildSize/domainCardinality`, which requires `ColumnStat.distinctCount`
   * (frequently None per SPARK-21043 / SPARK-30269); P2d ships size-based as
   * the always-available surrogate.
   */
  def rankBuilds(builds: Seq[LogicalPlan]): Seq[LogicalPlan] = {
    builds.sortBy { plan =>
      val size = plan.stats.sizeInBytes
      // Match the overflow-guarded conversion in shouldInject so sort ordering
      // matches gate decisions on the same plans.
      if (size.isValidLong) size.toLong else Long.MaxValue
    }
  }
}
