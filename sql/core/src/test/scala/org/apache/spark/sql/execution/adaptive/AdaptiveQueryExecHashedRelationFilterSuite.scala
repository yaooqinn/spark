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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.runtimefilter.{BroadcastedHashedRelationRef, HashedRelationContainsExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * P2b RED suite: AQE-aware HRC. These tests MUST fail at HEAD f3db2ce4a7d
 * (post-P2a-5d) because the AQE adapter does not yet exist. See todos
 * features/spark-hashed-relation-contains/docs/0005-investigation-p2b-aqe-audit.md
 * for the 4-axis peer-impl audit + SPIP cross-check that drives the design.
 *
 * Trigger: F2.1 SF100 q13 spike (HRC on, AQE default-on per Spark 3.2+)
 * hit SparkException [INTERNAL_ERROR] Cannot generate code for expression
 * hashedrelationcontains at HashedRelationContainsSubquery.doGenCode --
 * proves that without P2b the placeholder leaks through to codegen because
 * InsertAdaptiveSparkPlan wraps the plan as an opaque AdaptiveSparkPlanExec
 * before PlanHashedRelationContainsFilters can rewrite it.
 *
 * Expected GREEN landing: P2b implementation per audit doc 0005 section
 * "P2b implementation plan (post-audit, SPIP-aligned)" -- 2 new files
 * (SubqueryAdaptiveHashedRelationContainsExec +
 *  PlanAdaptiveHashedRelationContainsFilters) + 3 modified
 * (PlanAdaptiveSubqueries arm + AdaptiveSparkPlanExec rule slot +
 *  InsertAdaptiveSparkPlan.buildSubqueryMap match).
 */
class AdaptiveQueryExecHashedRelationFilterSuite extends QueryTest
  with SharedSparkSession with AdaptiveSparkPlanHelper {

  test("HRC fires under AQE on without crashing (P2b RED #1)") {
    // P2b RED #1: HRC on + AQE on (Spark 3.2+ default) must not crash.
    // Currently fails with SparkException [INTERNAL_ERROR] Cannot generate
    // code for expression hashedrelationcontains#N because the logical
    // HashedRelationContainsSubquery placeholder leaks through codegen
    // (InsertAdaptiveSparkPlan wraps the plan opaquely BEFORE
    // PlanHashedRelationContainsFilters can rewrite it).
    //
    // After P2b: PlanAdaptiveSubqueries arm + PlanAdaptiveHashedRelationContainsFilters
    // rewrite the placeholder during queryStageOptimizerRules pass.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val sqlStr =
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k"
        val baseline = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
          spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        }
        val withHrc = spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        assert(withHrc == baseline,
          s"HRC on (AQE on) must produce identical row set vs HRC off.\n" +
            s"  baseline (size=${baseline.size}): ${baseline.toSeq.sorted.take(20)}\n" +
            s"  withHrc  (size=${withHrc.size}):  ${withHrc.toSeq.sorted.take(20)}")
      }
    }
  }

  test("HRC reuse-fired plan-shape invariants under AQE (P2b RED #2)") {
    // P2b RED #2: P2a-5d sentinel #10 invariants must hold under AQE on.
    // Specifically: BHJ build broadcast and HRC ref must share ONE physical
    // broadcast exchange (no M1-shape silent regression) even when the
    // outer plan is wrapped in AdaptiveSparkPlanExec.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k")
        df.collect()
        // executedPlan after AQE returns the final post-materialization tree.
        val exec = df.queryExecution.executedPlan

        // Invariant 1: HRCExec present somewhere in the AQE plan.
        // Use the AdaptiveSparkPlanHelper.collectWithSubqueries which
        // descends through AdaptiveSparkPlanExec / QueryStageExec.
        val hrcExecs = collectWithSubqueries(exec) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        assert(hrcExecs.nonEmpty,
          s"Expected HashedRelationContainsExec under AQE.\nPlan:\n${exec.treeString}")

        // Invariant 2: exactly ONE BroadcastExchangeExec across AQE plan +
        // subqueries (shared between BHJ build and HRC ref). Use AQE-aware
        // helper so we descend into AdaptiveSparkPlanExec.executedPlan and
        // QueryStageExec.plan.
        val allBroadcastExchanges = collectWithSubqueries(exec) {
          case b: BroadcastExchangeExec => b
        }
        assert(allBroadcastExchanges.size == 1,
          s"HRC under AQE must reuse the BHJ BroadcastExchange. Found " +
            s"${allBroadcastExchanges.size} (expected 1).\nPlan:\n${exec.treeString}")

        // Invariant 3: at least one ReusedExchangeExec (reuse dedup fired
        // through ReuseAdaptiveSubquery rule, post-PlanAdaptiveHRCFilters).
        val reused = collectWithSubqueries(exec) {
          case r: ReusedExchangeExec => r
        }
        assert(reused.nonEmpty,
          s"Expected ReusedExchangeExec under AQE (reuse must fire).\n" +
            s"Plan:\n${exec.treeString}")
      }
    }
  }

  test("HRCExec.plan unwraps cleanly under AQE wrapper (P2b RED #3)") {
    // P2b RED #3: HRCExec.plan must end up as a BroadcastedHashedRelationRef
    // (possibly via ReusedSubqueryExec wrap) even under AQE. The dispatch
    // case in HashedRelationContainsExec.updateResult (0004 Gap D pattern
    // match) must continue to recognize the unwrapped ref. The AQE adapter
    // is responsible for producing a Ref whose child is the
    // AdaptiveSparkPlanExec-wrapped broadcast subplan (not a bare
    // BroadcastExchangeExec).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k")
        df.collect()
        val exec = df.queryExecution.executedPlan

        val hrcExecs = collectWithSubqueries(exec) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        assert(hrcExecs.nonEmpty,
          s"Need at least one HRCExec to inspect.\nPlan:\n${exec.treeString}")

        hrcExecs.foreach { e =>
          val isRef = e.plan.isInstanceOf[BroadcastedHashedRelationRef]
          val isReusedRef = e.plan match {
            case org.apache.spark.sql.execution.ReusedSubqueryExec(
              _: BroadcastedHashedRelationRef) => true
            case _ => false
          }
          assert(isRef || isReusedRef,
            s"HRCExec.plan under AQE must be BroadcastedHashedRelationRef " +
              s"or ReusedSubqueryExec(ref); got ${e.plan.getClass.getName}.\n" +
              s"Plan:\n${exec.treeString}")
        }
      }
    }
  }

  test("Composite int+int join under AQE produces same answer (P2c-1 B.8 RED #18)") {
    // P2c-1 B.8 (per stage2-r10 F2.2): composite-key HRC injection must work
    // under AQE-on path (PlanAdaptiveSubqueries -> PlanAdaptiveHashedRelationContainsFilters).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("b18", "p18") {
        spark.range(16).selectExpr(
          "cast(id % 4 as int) as k1",
          "cast(id / 4 as int) as k2",
          "id as v").createOrReplaceTempView("b18")
        spark.range(10000).selectExpr(
          "cast(id % 16 as int) as k1",
          "cast(id % 8 as int) as k2",
          "id as v").createOrReplaceTempView("p18")
        val sqlStr =
          "SELECT /*+ BROADCAST(b18) */ p18.v FROM p18 JOIN b18 ON " +
            "p18.k1 = b18.k1 AND p18.k2 = b18.k2"
        val baseline = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
          spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        }
        val df = spark.sql(sqlStr)
        val withHrc = df.collect().map(_.getLong(0)).toSet
        assert(withHrc == baseline,
          s"Composite HRC under AQE must equal HRC off.\n" +
            s"  baseline=${baseline.size}, withHrc=${withHrc.size}")
        // Inspect AFTER materialization so AQE has finalized; HRCExec lives
        // in the post-finalization plan, not the pre-AQE placeholder.
        val hrcExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        assert(hrcExecs.nonEmpty,
          s"Expected HRCExec under AQE composite-key path.\n" +
            s"Plan:\n${df.queryExecution.executedPlan.treeString}")
      }
    }
  }
}
