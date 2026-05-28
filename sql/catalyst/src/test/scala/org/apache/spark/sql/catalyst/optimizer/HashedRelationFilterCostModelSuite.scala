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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.catalyst.optimizer.HashedRelationFilterCostModel.SkipReasons._
import org.apache.spark.sql.internal.SQLConf

/**
 * Unit tests for [[HashedRelationFilterCostModel]] gates and ranking.
 *
 * Cost model is read-only on the per-query `filterCounter`: the caller (rule)
 * passes the current value, the cost model uses it to decide
 * `per-query-budget-exhausted`, but never mutates the counter. The caller
 * increments after a successful inject -- this mirrors the peer
 * `InjectRuntimeFilter.tryInjectRuntimeFilter` shape.
 */
class HashedRelationFilterCostModelSuite extends PlanTest {

  private val a = $"a".int
  private val b = $"b".int

  /** Stats-bearing plan via the catalyst `StatsTestPlan` (peer convention). */
  private def plan(rows: Long, sizeInBytes: Option[Long] = None): StatsTestPlan = {
    StatsTestPlan(
      outputList = Seq(a, b),
      rowCount = BigInt(rows),
      attributeStats = AttributeMap.empty,
      size = sizeInBytes.map(BigInt(_)).orElse(Some(BigInt(rows * 16L))))
  }

  /** Plan with no rowCount stats (CBO-off / no column stats). */
  private def planWithoutStats(): LocalRelation = LocalRelation(a, b)

  test("build-stats-unavailable: Inject with hint when build rowCount is None " +
    "(CBO off or no column stats; fail-open)") {
    val build = planWithoutStats() // rowCount = None
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_CREATION_SIDE_THRESHOLD, Long.MaxValue)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Inject],
      s"Inject expected when stats missing (fail-open), got $decision")
    assert(decision.reason.contains("build-stats-unavailable"),
      s"Inject reason should carry build-stats-unavailable hint, got '${decision.reason}'")
  }

  test("MaxBuildSize gate: Skip when build rowCount exceeds threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, 1L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build rowCount > 1, got $decision")
    assert(decision.reason.startsWith(MaxBuildRowsExceeded + ":"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("MinApplicationSize gate: Skip when probe rowCount below threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 1000000L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when probe rowCount < threshold, got $decision")
    assert(decision.reason.startsWith(MinApplicationRowsNotMet + ":"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("MaxFiltersPerQuery gate: Skip when filterCounter at limit") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_QUERY, 2)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 2, conf) // already at cap
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when filterCounter at limit, got $decision")
    assert(decision.reason.startsWith(PerQueryBudgetExhausted + ":"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("MaxFiltersPerQuery gate: Inject when filterCounter below limit") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_QUERY, 2)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 1, conf) // below cap
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Inject],
      s"Inject expected when filterCounter below limit, got $decision")
  }

  test("CreationSideThreshold gate: Skip when build sizeInBytes exceeds threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_QUERY, Int.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_CREATION_SIDE_THRESHOLD, 0L)
    val sizedBuild = plan(rows = 1L, sizeInBytes = Some(1024L))
    val decision = HashedRelationFilterCostModel.shouldInject(
      sizedBuild, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build sizeInBytes > threshold, got $decision")
    assert(decision.reason.startsWith(CreationSideThresholdExceeded + ":"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("broadcastability gate: Skip with `build-not-broadcastable` when build cannot broadcast") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = false, probeBroadcastable = false,
      filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build not broadcastable, got $decision")
    assert(decision.reason == BuildNotBroadcastable,
      s"reason mismatch, got '${decision.reason}'")
  }

  test("broadcastability gate: Skip with `probe-broadcastable` when probe is broadcastable") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = true,
      filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when probe is broadcastable, got $decision")
    assert(decision.reason == ProbeBroadcastable,
      s"reason mismatch, got '${decision.reason}'")
  }

  test("MaxFiltersPerQuery gate: 2-call sequence pins counter-advances-across-sites " +
    "(not reset)") {
    // H4 (Stage 5 P2e r1): proves the per-query counter semantics survive a
    // future refactor that might accidentally reset `filterCounter` inside the
    // rule's transformWithPruning arm. Set cap=1 and simulate two sequential
    // gate calls: the first (counter=0) Injects; the second (counter=1, as the
    // caller would pass after the first inject) must Skip with the
    // per-query-budget-exhausted reason. A counter-reset bug would let the
    // second call Inject again.
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_QUERY, 1)
    val firstCall = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 0, conf)
    assert(firstCall.isInstanceOf[HashedRelationFilterCostModel.Inject],
      s"First call (filterCounter=0, cap=1) should Inject, got $firstCall")
    val secondCall = HashedRelationFilterCostModel.shouldInject(
      build, probe,
      buildBroadcastable = true, probeBroadcastable = false,
      filterCounter = 1, conf)
    assert(secondCall.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Second call (filterCounter=1, cap=1) should Skip, got $secondCall")
    assert(secondCall.reason.startsWith(PerQueryBudgetExhausted + ":"),
      s"Second-call Skip reason should pin per-query-budget-exhausted " +
        s"(else a counter-reset bug would silently pass), got '${secondCall.reason}'")
  }

  test("Decision ADT exhaustively pattern-matchable as sealed trait") {
    val decisions: Seq[HashedRelationFilterCostModel.Decision] = Seq(
      HashedRelationFilterCostModel.Inject("test-inject"),
      HashedRelationFilterCostModel.Skip("test-skip"))
    decisions.foreach { d =>
      val reason = d match {
        case HashedRelationFilterCostModel.Inject(r) => r
        case HashedRelationFilterCostModel.Skip(r) => r
      }
      assert(reason.nonEmpty)
    }
  }

  test("rankBuilds: empty input returns empty") {
    assert(HashedRelationFilterCostModel.rankBuilds(Seq.empty).isEmpty)
  }

  test("rankBuilds: single-element input returns the same element") {
    val p = plan(100)
    val ranked = HashedRelationFilterCostModel.rankBuilds(Seq(p))
    assert(ranked.size == 1)
    assert(ranked.head eq p)
  }

  test("rankBuilds: three-element input is sorted by sizeInBytes ascending") {
    val small = plan(10L)   // size = 160
    val mid = plan(50L)     // size = 800
    val large = plan(20L, sizeInBytes = Some(10000L)) // overrides default
    val ranked = HashedRelationFilterCostModel.rankBuilds(Seq(large, small, mid))
    assert(ranked == Seq(small, mid, large))
  }
}
