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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
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

  private def plan(rows: Int): LocalRelation = {
    // LocalRelation.computeStats reports rowCount = None by default. Override
    // so tests can isolate gates that depend on rowCount being present
    // (MaxBuildSize / MinApplicationSize). Pass rows = -1 to keep the default
    // None (used by the build-stats-unavailable Inject test).
    if (rows < 0) {
      LocalRelation(a, b)
    } else {
      new LocalRelation(Seq(a, b)) {
        override def computeStats(): org.apache.spark.sql.catalyst.plans.logical.Statistics =
          org.apache.spark.sql.catalyst.plans.logical.Statistics(
            sizeInBytes = BigInt(rows.toLong * 16L), // 2 LongType cols
            rowCount = Some(BigInt(rows.toLong)))
      }
    }
  }

  test("build-stats-unavailable: Inject with hint when build rowCount is None " +
    "(CBO off or no column stats; fail-open)") {
    val build = plan(-1) // rowCount = None
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 0L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_CREATION_SIDE_THRESHOLD, Long.MaxValue)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, filterCounter = 0, conf)
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
      build, probe, filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build rowCount > 1, got $decision")
    assert(decision.reason.startsWith("max-build-rows-exceeded:"),
      s"reason prefix mismatch, got '${decision.reason}'")
  }

  test("MinApplicationSize gate: Skip when probe rowCount below threshold") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE, 1000000L)
    conf.setConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_MAX_BUILD_SIZE, Long.MaxValue)
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when probe rowCount < threshold, got $decision")
    assert(decision.reason.startsWith("min-application-rows-not-met:"),
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
      build, probe, filterCounter = 2, conf) // already at cap
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when filterCounter at limit, got $decision")
    assert(decision.reason.startsWith("per-query-budget-exhausted:"),
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
      build, probe, filterCounter = 1, conf) // below cap
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
    val sizedBuild = new LocalRelation(Seq(a, b)) {
      override def computeStats(): org.apache.spark.sql.catalyst.plans.logical.Statistics =
        org.apache.spark.sql.catalyst.plans.logical.Statistics(
          sizeInBytes = BigInt(1024L),
          rowCount = Some(BigInt(1L)))
    }
    val decision = HashedRelationFilterCostModel.shouldInject(
      sizedBuild, probe, filterCounter = 0, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"Skip expected when build sizeInBytes > threshold, got $decision")
    assert(decision.reason.startsWith("creation-side-threshold-exceeded:"),
      s"reason prefix mismatch, got '${decision.reason}'")
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

  test("rankBuilds: three-element input is a stable sort by sizeInBytes ascending") {
    val plans = Seq(plan(10), plan(50), plan(20))
    val ranked = HashedRelationFilterCostModel.rankBuilds(plans)
    assert(ranked.size == 3)
    // All sizes equal in the LocalRelation stub -> stable sort preserves order
    assert(ranked == plans)
  }
}
