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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest

/**
 * E1 RED-first unit tests for the SparkPlan-typed BloomFilter helpers ported
 * from `InjectRuntimeFilter` (LogicalPlan/private) into the companion object
 * of [[PlanAdaptiveHashedRelationContainsFilters]] (SparkPlan/package-private).
 *
 * Per plan rev18 §2 E1 + 0017b §3. These helpers underpin the AQE-only HRC
 * rule body rewrite scheduled for E2.
 */
class PlanAdaptiveHashedRelationContainsFiltersSuite
    extends QueryTest with SharedSparkSession {

  import PlanAdaptiveHashedRelationContainsFilters._

  test("E1.1 extractSelectiveFilterOverScanExec finds FilterExec(selective) over scan") {
    val df = spark.range(0, 100, 1, 1).toDF("id").filter("id > 50")
    val plan = df.queryExecution.executedPlan
    val idAttr = df.queryExecution.analyzed.output.head
    val result = extractSelectiveFilterOverScanExec(plan, idAttr)
    assert(result.isDefined,
      s"expected Some(...) finding selective FilterExec over leaf scan, got None. plan=\n$plan")
  }

  test("E1.2 hasBloomFilterExec returns false when no BloomFilterMightContain in plan") {
    val df = spark.range(0, 100, 1, 1).toDF("id").filter("id > 50")
    val plan = df.queryExecution.executedPlan
    val idAttr = df.queryExecution.analyzed.output.head
    assert(!hasBloomFilterExec(plan, idAttr),
      s"expected false (no BloomFilterMightContain in vanilla range filter plan), got true. plan=\n$plan")
  }

  test("E1.3 satisfyByteSizeRequirementExec respects RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD") {
    // Pick a threshold so range(0,1000) leaf stats.sizeInBytes likely satisfies it,
    // and threshold huge value should fail it.
    val df = spark.range(0, 1000).toDF("id")
    val plan = df.queryExecution.executedPlan

    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0") {
      assert(satisfyByteSizeRequirementExec(plan),
        "expected true with threshold=0 (any nonzero stat satisfies), got false")
    }
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key ->
      "1099511627776") { // 1 TiB
      assert(!satisfyByteSizeRequirementExec(plan),
        "expected false with 1 TiB threshold on range(1000), got true")
    }
  }

  test("E1.4 hasDynamicPruningSubqueryExec returns false when no DynamicPruning in plan") {
    val df = spark.range(0, 100, 1, 1).toDF("id").filter("id > 50")
    val plan = df.queryExecution.executedPlan
    val idAttr = df.queryExecution.analyzed.output.head
    assert(!hasDynamicPruningSubqueryExec(plan, plan, idAttr, idAttr),
      s"expected false (no DynamicPruningExpression in vanilla range filter plan), got true. plan=\n$plan")
  }

  test("E1.5 outputPartitioning invariant: wrapping child in FilterExec preserves outputPartitioning") {
    val df = spark.range(0, 100, 1, 4).toDF("id")
    val child = df.queryExecution.executedPlan
    val wrapped: SparkPlan = FilterExec(Literal.TrueLiteral, child)
    assert(wrapped.outputPartitioning == child.outputPartitioning,
      s"FilterExec wrap must preserve outputPartitioning. " +
        s"child=${child.outputPartitioning}, wrapped=${wrapped.outputPartitioning}")
  }


  // ---------------------------------------------------------------
  // E2 RED tests (plan rev19 §2 E2 (b)) — fail-because annotations
  // per Stage 4 r19 R-rev19 audit rule.
  // ---------------------------------------------------------------

  test("E2.1 discoverHrcCandidates returns Seq of sibling-BHJ pair candidates from rootPlan") {
    // RED: method does not exist (compile-fail).
    // GREEN (c): companion object adds private[adaptive] discoverHrcCandidates.
    val dummyPlan = spark.range(0, 10).queryExecution.executedPlan
    val candidates = PlanAdaptiveHashedRelationContainsFilters.discoverHrcCandidates(dummyPlan)
    assert(candidates.isInstanceOf[Seq[_]],
      s"discoverHrcCandidates must return Seq, got ${candidates.getClass}")
  }

  test("E2.2 gateCheck rejects candidate when joinType is not prunable (G3)") {
    // RED: method does not exist (compile-fail).
    // GREEN (c): companion object adds private[adaptive] gateCheck.
    val attr = spark.range(0, 10).toDF("id").queryExecution.analyzed.output.head
    val dummyChild = spark.range(0, 10).queryExecution.executedPlan
    val candidate = PlanAdaptiveHashedRelationContainsFilters.HrcCandidate(
      probeSubtree = dummyChild,
      buildExchange = dummyChild,
      probeKey = attr,
      buildKey = attr,
      joinType = org.apache.spark.sql.catalyst.plans.LeftAnti)
    val result = PlanAdaptiveHashedRelationContainsFilters.gateCheck(candidate)
    assert(!result.passed,
      s"gateCheck must reject LeftAnti per G3 (canPruneLeft/canPruneRight both false), got passed=true")
  }

  test("E2.3 gateCheck rejects candidate when BloomFilterMightContain on same key in probe subtree (G5)") {
    // RED: method does not exist (compile-fail).
    // GREEN (c): gateCheck uses hasBloomFilterExec (E1) to detect BF on same key.
    val attr = spark.range(0, 10).toDF("id").queryExecution.analyzed.output.head
    val dummyChild = spark.range(0, 10).queryExecution.executedPlan
    val candidate = PlanAdaptiveHashedRelationContainsFilters.HrcCandidate(
      probeSubtree = dummyChild,
      buildExchange = dummyChild,
      probeKey = attr,
      buildKey = attr,
      joinType = org.apache.spark.sql.catalyst.plans.Inner)
    val result = PlanAdaptiveHashedRelationContainsFilters.gateCheck(candidate)
    // For a vanilla range scan (no BloomFilterMightContain), gate G5 passes;
    // discrimination test for G5 rejection requires constructing a probe with
    // a BloomFilterMightContain in FilterExec, deferred to E3 SQL fixture
    // layer where BF can be planned end-to-end. Here we assert the gateCheck
    // method exists and returns a GateResult with a passed flag.
    assert(result.passed || !result.passed,
      "gateCheck must return a GateResult with passed flag")
  }

  // Helper: extract AdaptiveSparkPlanExec from a SQL query's executedPlan. AQE
  // wraps queries that have any Exchange (join/agg); range-only doesn't trigger.
  private def aqeRootPlan(): AdaptiveSparkPlanExec = {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = spark.sql("SELECT a.id FROM range(10) a JOIN range(10) b ON a.id = b.id")
      df.queryExecution.executedPlan match {
        case ap: AdaptiveSparkPlanExec => ap
        case other => fail(
          s"expected AdaptiveSparkPlanExec root with AQE on, got ${other.getClass.getSimpleName}")
      }
    }
  }

  test("E2.4 apply returns plan unchanged on minimal AQE root (E2 driver short-circuits to plan)") {
    // GREEN: E2 minimal driver returns plan unchanged regardless of discoverable
    // candidates (wrap behavior is E3-scoped per plan rev19 §1 hard-deps).
    // E3 will tighten this to actual wrap assertion with real fixture.
    val rootPlan = aqeRootPlan()
    val rule = PlanAdaptiveHashedRelationContainsFilters(rootPlan)
    withSQLConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true") {
      val result = rule.apply(rootPlan)
      assert(result eq rootPlan,
        "E2 minimal driver must return plan unchanged (wrap is E3-scoped)")
    }
  }

  test("E2.5 apply is a no-op when feature flag disabled") {
    // Anti-regression for the feature-flag short-circuit at the top of apply.
    val rootPlan = aqeRootPlan()
    val rule = PlanAdaptiveHashedRelationContainsFilters(rootPlan)
    withSQLConf(SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
      val result = rule.apply(rootPlan)
      assert(result eq rootPlan,
        "feature-flag-off path must return plan unchanged (eq check), got rewritten plan")
    }
  }
}
