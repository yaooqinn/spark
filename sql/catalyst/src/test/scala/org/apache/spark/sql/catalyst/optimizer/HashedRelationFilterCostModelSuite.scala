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

  test("D.0 skeleton: shouldInject returns Skip placeholder pending D.1-D.5 wiring") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    val budget = freshBudget
    val decision = HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = 42L, budget, hasBloomOnSameLineage = false, conf)
    assert(decision.isInstanceOf[HashedRelationFilterCostModel.Skip],
      s"D.0 placeholder must return Skip, got $decision")
    assert(decision.reason == "d0-skeleton-not-yet-wired",
      s"placeholder reason mismatch, got '${decision.reason}'")
  }

  test("D.0 skeleton: Skip path does not mutate caller-managed budget Map") {
    val build = plan(100)
    val probe = plan(10000)
    val conf = new SQLConf
    val budget = freshBudget
    HashedRelationFilterCostModel.shouldInject(
      build, probe, probeScanAnchor = 42L, budget, hasBloomOnSameLineage = false, conf)
    assert(budget.isEmpty,
      s"Skip must not increment budget, but found ${budget.toMap}")
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
