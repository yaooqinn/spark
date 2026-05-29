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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Cost model gating HashedRelationContains injection. Pure decisions; holds no
 * mutable state. Mirrors `InjectRuntimeFilter`'s gate-counter shape: the caller
 * threads a per-query `filterCounter` and the cost model is read-only on it.
 */
private[sql] object HashedRelationFilterCostModel {

  /** Stable Skip-reason prefixes; tests assert on these via `startsWith`. */
  object SkipReasons {
    val BuildNotBroadcastable = "build-not-broadcastable"
    val ProbeBroadcastable = "probe-broadcastable"
    val PerQueryBudgetExhausted = "per-query-budget-exhausted"
    val MaxBuildRowsExceeded = "max-build-rows-exceeded"
    val CreationSideThresholdExceeded = "creation-side-threshold-exceeded"
    val MinApplicationRowsNotMet = "min-application-rows-not-met"
  }

  /** Result of a cost-model gating decision. */
  sealed trait Decision { def reason: String }
  final case class Inject(reason: String) extends Decision
  final case class Skip(reason: String) extends Decision

  /**
   * Cost-model gates, in order:
   *   1. broadcastability of build and probe (caller-computed via
   *      `JoinSelectionHelper.canBroadcastBySize`),
   *   2. per-query budget (`filterCounter` vs `maxFiltersPerQuery`),
   *   3. build row count vs `maxBuildSize`,
   *   4. build sizeInBytes vs `creationSideThreshold`,
   *   5. probe row count vs `minApplicationSize`.
   *
   * Order rationale: user-intent / counter gates fire before stats-dependent
   * gates so a user-disabled (e.g. cap = 0) configuration short-circuits
   * without three stats reads. The Bloom mutex is performed by the caller
   * after a positive decision.
   *
   * CreationSideThreshold is checked on `stats.sizeInBytes`; MaxBuildSize is
   * checked on `stats.rowCount`. The two are complementary (wide rows vs
   * narrow-many-rows). When `stats.sizeInBytes` equals the no-stats sentinel
   * `conf.defaultSizeInBytes`, we fail open: the canBroadcastBySize gate
   * (already checked above) bounds the build size, so we do not Skip on
   * missing stats alone.
   */
  def shouldInject(
      buildPlan: LogicalPlan,
      probePlan: LogicalPlan,
      buildBroadcastable: Boolean,
      probeBroadcastable: Boolean,
      filterCounter: Int,
      conf: SQLConf): Decision = {
    import SkipReasons._
    if (!buildBroadcastable) return Skip(BuildNotBroadcastable)
    if (probeBroadcastable) return Skip(ProbeBroadcastable)
    val maxFiltersPerQuery = conf.runtimeFilterHashedRelationContainsMaxFiltersPerQuery
    if (filterCounter >= maxFiltersPerQuery) {
      return Skip(s"$PerQueryBudgetExhausted: injected=$filterCounter " +
        s"limit=$maxFiltersPerQuery")
    }
    val buildSizeInBytesBig = buildPlan.stats.sizeInBytes
    val buildRowCountOpt = buildPlan.stats.rowCount.map(_.toLong)
    // Fail-open on missing build rowCount (CBO off): treat unknown as 0 rows
    // so MaxBuildSize does not silently Skip every probe site in a CBO-off
    // cluster. The downstream canBroadcastBySize gate (already checked above)
    // bounds the build size.
    val buildRowCount = buildRowCountOpt.getOrElse(0L)
    // BigInt.toLong is mod 2^64 with no overflow signal; isValidLong guards
    // pathological multiplicative stats estimates from silent truncation.
    val buildSizeInBytes =
      if (buildSizeInBytesBig.isValidLong) buildSizeInBytesBig.toLong else Long.MaxValue
    // probe rowCount missing -> fail-open (skip the minApplicationSize gate).
    // Asymmetric with maxBuildSize: a missing build rowCount means "don't know
    // how big the broadcast is" and we let it through; a missing probe rowCount
    // means "don't know if the probe is large enough to be worth filtering"
    // and we likewise let it through. Both choices favor injecting when stats
    // are absent, consistent with `LogicalPlan.stats.rowCount` being
    // frequently None pre-CBO (SPARK-21043 / SPARK-30269).
    val probeRowCountOpt = probePlan.stats.rowCount.map(_.toLong)
    val maxBuildRows = conf.runtimeFilterHashedRelationContainsMaxBuildSize
    val creationSideThresholdBytes =
      conf.runtimeFilterHashedRelationContainsCreationSideThreshold
    val minAppRows = conf.runtimeFilterHashedRelationContainsMinApplicationSize
    if (buildRowCount > maxBuildRows) {
      Skip(s"$MaxBuildRowsExceeded: $buildRowCount > $maxBuildRows")
    } else if (buildSizeInBytes > creationSideThresholdBytes) {
      Skip(s"$CreationSideThresholdExceeded: $buildSizeInBytes > " +
        s"$creationSideThresholdBytes bytes")
    } else if (probeRowCountOpt.exists(_ < minAppRows)) {
      Skip(s"$MinApplicationRowsNotMet: ${probeRowCountOpt.get} < $minAppRows")
    } else {
      // Lazy-build the Inject reason only if the caller asks for it. logDebug
      // already evaluates lazily, but the caller (rule) typically discards the
      // reason on Inject and we save the string concat.
      val probeStatsHint = probeRowCountOpt match {
        case Some(n) => n.toString
        case None => "?"
      }
      val statsHint =
        (if (buildRowCountOpt.isEmpty) " (build-stats-unavailable)" else "") +
        (if (probeRowCountOpt.isEmpty) " (probe-stats-unavailable)" else "")
      Inject(s"all-gates-passed: buildRows=$buildRowCount " +
        s"buildBytes=$buildSizeInBytes probeRows=$probeStatsHint " +
        s"injected=$filterCounter/$maxFiltersPerQuery$statsHint")
    }
  }

  /** @VisibleForTesting -- not currently called from production. */
  def rankBuilds(builds: Seq[LogicalPlan]): Seq[LogicalPlan] = {
    builds.sortBy { plan =>
      val size = plan.stats.sizeInBytes
      if (size.isValidLong) size.toLong else Long.MaxValue
    }
  }
}
