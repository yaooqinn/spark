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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.HashedRelationContainsSubquery
import org.apache.spark.sql.catalyst.optimizer.InjectHashedRelationFilters
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.runtimefilter.{BroadcastedHashedRelationRef, HashedRelationContainsExec, PlanHashedRelationContainsFilters}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for the new HRC injection rule (SPARK-XXXXX, HRC PR #1, Core MVP).
 *
 * Tracks the first RED slices of P2a per todos
 * features/spark-hashed-relation-contains/docs/0003-implementation-plan.md rev 8.
 * Behavioral tests (BHJ-injects-HRC, plan-shape, reuse) land alongside the
 * actual detection + rewrite logic. The tests in this file anchor compile-time
 * REDs that prove the four scaffolded classes (rule + Subquery + Exec + Ref)
 * exist with their contracted signatures.
 */
class InjectHashedRelationFiltersSuite extends SharedSparkSession {

  test("InjectHashedRelationFilters rule object exists in catalyst.optimizer") {
    // P2a RED #1: the rule must be a registered Catalyst optimizer object.
    // Until the production class lands, this import fails to compile, which
    // is the intended RED signal (per AGENTS.md TDD rule A.4: RED guards the
    // production entry, not stdlib behavior).
    assert(InjectHashedRelationFilters.ruleName ==
      "org.apache.spark.sql.catalyst.optimizer.InjectHashedRelationFilters")
  }

  test("HashedRelationContainsSubquery class exists with the contracted signature") {
    // P2a RED #2: anchor the logical SubqueryExpression node. We do not build a
    // real subquery here (that requires a child plan and resolved keys); the
    // import alone validates the class is reachable from sql.catalyst.expressions.
    assert(classOf[HashedRelationContainsSubquery].getSimpleName ==
      "HashedRelationContainsSubquery")
  }

  test("BroadcastedHashedRelationRef class exists in execution.runtimefilter") {
    // P2a RED #3: anchor the no-collect physical ref node. The actual broadcast()
    // call requires a fully planned SparkPlan child; this test just proves the
    // class is in the contracted package per 0002c-contract.md §3.2.
    assert(classOf[BroadcastedHashedRelationRef].getPackage.getName ==
      "org.apache.spark.sql.execution.runtimefilter")
  }

  test("HashedRelationContainsExec class exists in execution.runtimefilter") {
    // P2a RED #4: anchor the probe-side predicate. doGenCode + eval bodies land
    // in the next slice (per stage2-r6 F3.4: CodegenFallback vs doGenCode lock
    // forced at P2a RED time).
    assert(classOf[HashedRelationContainsExec].getPackage.getName ==
      "org.apache.spark.sql.execution.runtimefilter")
  }

  test("HRC SQLConf keys exposed with documented defaults (P2a-3)") {
    // P2a-3 RED #5: six SQLConf keys per docs/0002c-contract.md §2.
    // Default values match the contract verbatim; documentation and
    // version strings are checked by the AllSparkConf golden file.
    val conf = SQLConf.get
    assert(!conf.runtimeFilterHashedRelationContainsEnabled,
      "enabled default should be false until PR #5 flip")
    assert(conf.runtimeFilterHashedRelationContainsMinApplicationSize == 10000L)
    assert(conf.runtimeFilterHashedRelationContainsMaxBuildSize == 1000000L)
    assert(conf.runtimeFilterHashedRelationContainsMaxFiltersPerScan == 8)
    assert(conf.runtimeFilterHashedRelationContainsCreationSideThreshold == 10L * 1024 * 1024)
    assert(conf.runtimeFilterHashedRelationContainsBloomMutualExclusion)
  }

  test("InjectHashedRelationFilters injects HRC subquery on BHJ probe side (P2a-4)") {
    // P2a-4 RED #6: the first behavioral RED. Constructs a tiny equi-join where
    // one side is broadcastable and the other is not; expects the rule to wrap
    // the probe-side scan in a Filter(HashedRelationContainsSubquery(...)).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      // Disable Bloom so its inject doesn't perturb the assertion. HRC is the
      // only runtime filter under test in this slice.
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {

      // Small build side (broadcastable under 5000-byte threshold) joined with
      // a synthetic 10_000-row probe (not broadcastable). The rule should inject
      // a HashedRelationContainsSubquery on the probe-side Range scan.
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.nonEmpty,
          s"Expected at least one HashedRelationContainsSubquery in the optimized plan, " +
            s"but found none.\nPlan:\n${optimized.treeString}")
      }
    }
  }

  test("PlanHashedRelationContainsFilters rule object exists (P2a-5a RED #7)") {
    // P2a-5a RED #7: existence + ruleName anchor for the new physical preparations
    // rule. Identity scaffold in this slice; real apply (sameResult reuse +
    // BroadcastExchangeExec wrap + Filter(HRCExec) rewrite) lands in P2a-5b.
    val expected =
      "org.apache.spark.sql.execution.runtimefilter.PlanHashedRelationContainsFilters"
    assert(PlanHashedRelationContainsFilters(spark).ruleName == expected)
  }

  test("PlanHashedRelationContainsFilters rewrites placeholder to HRCExec (P2a-5b RED #8)") {
    // P2a-5b RED #8: behavioral RED for physical rewrite. After preparations,
    // the logical HashedRelationContainsSubquery placeholder must be eliminated
    // and replaced by a HashedRelationContainsExec wrapping a
    // BroadcastedHashedRelationRef whose child is the sibling BHJ's
    // BroadcastExchangeExec (sameResult reuse). End-to-end .collect() not
    // exercised here because HashedRelationContainsExec.eval/doGenCode remain
    // scaffold UOEs until P2a-5c.
    //
    // AQE must be off for this slice: InsertAdaptiveSparkPlan is also a
    // preparations rule and wraps everything as a leaf AdaptiveSparkPlanExec,
    // causing all subsequent preparations rules (including ours) to no-op.
    // AQE-aware HRC rewrite lands in P2b (PlanAdaptiveHashedRelationContainsFilters).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k")
        val executed = df.queryExecution.executedPlan

        // 1. Logical placeholder must NOT survive into the executed plan.
        val survivedPlaceholders = executed.flatMap { sp =>
          sp.expressions.flatMap(_.collect { case s: HashedRelationContainsSubquery => s })
        }
        assert(survivedPlaceholders.isEmpty,
          s"HashedRelationContainsSubquery placeholder should be rewritten by " +
            s"PlanHashedRelationContainsFilters but survived.\nPlan:\n${executed.treeString}")

        // 2. HashedRelationContainsExec wrapping a BroadcastedHashedRelationRef
        //    must appear (rewrite landed).
        val hrcExecs = executed.flatMap { sp =>
          sp.expressions.flatMap(_.collect { case e: HashedRelationContainsExec => e })
        }
        assert(hrcExecs.nonEmpty,
          s"Expected at least one HashedRelationContainsExec after preparations, " +
            s"but found none.\nPlan:\n${executed.treeString}")
        assert(hrcExecs.forall(_.ref.isInstanceOf[BroadcastedHashedRelationRef]),
          "Every HashedRelationContainsExec must carry a BroadcastedHashedRelationRef.")
      }
    }
  }
}
