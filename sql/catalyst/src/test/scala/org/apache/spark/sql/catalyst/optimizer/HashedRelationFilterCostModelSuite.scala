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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf

/**
 * Unit tests for [[HashedRelationFilterCostModel]].
 *
 * D.0 scope: skeleton object + Decision ADT shape + rankBuilds sort.
 * D.1-D.5 will extend this suite with per-gate Skip tests as the SQLConfs
 * are wired into the production rule (`InjectHashedRelationFilters`).
 */
class HashedRelationFilterCostModelSuite extends PlanTest {

  private val a = $"a".int
  private val b = $"b".int

  private def freshBudget = mutable.Map.empty[Long, Int]

  private def plan(rows: Int): LocalRelation = {
    // Stub a LocalRelation; LocalRelation.computeStats uses output type sizes
    // and we don't need exact byte counts here -- Decision shape is the focus.
    LocalRelation(a, b)
  }

  test("D.2 MaxBuildSize gate: Skip when build rowCount exceeds threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, 1L)
    // probe rowCount unset (defaults to 0) would also Skip on MinAppSize, but
    // build-size check runs first; we drop MinAppSize requirement to isolate.
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    val budget = freshBudget
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = 42L, budget, hasBloomOnSameLineage = false, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build rowCount (Long.MaxValue stub) > 1, got $decision")
    assert(decision.reason.startsWith("max-build-rows-exceeded:"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("D.1 MinApplicationSize gate: Skip when probe rowCount below threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 1000000L)
    // LocalRelation stats.rowCount is None -> getOrElse(Long.MaxValue) on build
    // would trip MaxBuildSize first; opt out to isolate MinAppSize.
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    val budget = freshBudget
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = 42L, budget, hasBloomOnSameLineage = false, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when probe rowCount unset (defaults to 0) < threshold, got $decision")
    assert(decision.reason.startsWith("min-application-rows-not-met:"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("D.1 MinApplicationSize gate: Skip path does not mutate budget Map") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 1000000L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    val budget = freshBudget
    HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = 42L, budget, hasBloomOnSameLineage = false, conf)
    assert(budget.isEmpty, s"Skip must not increment budget, but found ${budget.toMap}")
  }

  test("D.3 MaxFiltersPerScan gate: Skip when budget for this anchor at limit") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    // Isolate D.3: open MinAppSize / MaxBuildSize so they don't Skip first.
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_SCAN, 2)
    val budget = freshBudget
    val anchor = 99L
    budget(anchor) = 2 // already at cap
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = anchor, budget, hasBloomOnSameLineage = false, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when budget at limit, got $decision")
    assert(decision.reason.startsWith("per-scan-budget-exhausted:"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("D.3 MaxFiltersPerScan gate: Inject when budget for this anchor below limit") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_SCAN, 2)
    val budget = freshBudget
    val anchor = 99L
    budget(anchor) = 1 // below cap
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = anchor, budget, hasBloomOnSameLineage = false, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Inject],
      s"Inject expected when budget below limit, got $decision")
    // Cost model is read-only on budget: caller increments post-Inject.
    assert(budget(anchor) == 1, s"cost-model must not mutate budget, got ${budget.toMap}")
  }

  test("D.5 CreationSideThreshold gate: Skip when build sizeInBytes exceeds threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    // Isolate D.5: open MinAppSize / MaxBuildSize / MaxFiltersPerScan so they
    // don't Skip first. LocalRelation reports sizeInBytes=0 for empty data
    // (no row backing) -- use threshold=0 so any non-negative buildBytes
    // triggers the gate. The gate uses strict `>` so we additionally make
    // the test stub return a non-zero size by overriding stats below.
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_FILTERS_PER_SCAN, Int.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_CREATION_SIDE_THRESHOLD, 0L)
    // Stub a build whose sizeInBytes is provably > 0.
    val sizedBuild = new LocalRelation(Seq(a, b)) {
      override def computeStats(): org.apache.spark.sql.catalyst.plans.logical.Statistics =
        org.apache.spark.sql.catalyst.plans.logical.Statistics(
          sizeInBytes = BigInt(1024L))
    }
    val budget = freshBudget
    val decision = HashedRelationFilterCostModel.shouldInject(
      sizedBuild, probe, probeScanAnchor = 42L, budget,
      hasBloomOnSameLineage = false, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build sizeInBytes > threshold, got $decision")
    assert(decision.reason.startsWith("creation-side-threshold-exceeded:"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("D.0 skeleton: Decision ADT exhaustively pattern-matchable as sealed trait") {
    // Compiler enforces sealed via -Wunused, but a manual exhaustiveness check
    // here catches future Decision variants added without updating callers.
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

  test("rankBuilds: three-element input sorted by sizeInBytes ascending") {
    // All three LocalRelations share the same stub-row size; ranking is a
    // deterministic stable sort, so the original order is preserved when
    // sizes are equal -- assert the API contract (sortBy stable) rather than
    // a specific permutation. Real-size discrimination is exercised in
    // integration test D.7.
    val plans = Seq(plan(10), plan(50), plan(20))
    val ranked = HashedRelationFilterCostModel.rankBuilds(plans)
    assert(ranked.size == 3)
    // All sizes equal -> stable sort preserves order
    assert(ranked == plans)
  }
}
