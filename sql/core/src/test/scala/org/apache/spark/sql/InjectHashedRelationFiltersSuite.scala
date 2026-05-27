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
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
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
        assert(hrcExecs.forall(_.plan.isInstanceOf[BroadcastedHashedRelationRef]),
          "Every HashedRelationContainsExec must carry a BroadcastedHashedRelationRef.")
      }
    }
  }

  test("HRC end-to-end produces same answer as HRC off (P2a-5c-r2 RED #9)") {
    // P2a-5c-r2 RED #9 (re-attempt after P2a-5c retract — see todos
    // features/spark-hashed-relation-contains/docs/0002c-contract.md rev 2
    // section 3.3 + docs/0004-investigation-peer-audit-pass.md for the
    // ExecSubqueryExpression-based redesign). The query must collect the
    // same row set with HRC enabled vs disabled. Until the rev 2 §3.3
    // implementation lands (ExecSubqueryExpression mixin + updateResult
    // hook + plan: BaseSubqueryExec field + canonicalized impl), this RED
    // either throws UOE (current scaffold) or NPE (rev 1 shape, retracted).
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val sqlStr =
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k"
        val baseline = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
          spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        }
        val withHrc = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true") {
          spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        }
        assert(withHrc == baseline,
          s"HRC on must produce identical row set vs HRC off.\n" +
            s"  baseline (HRC off, size=${baseline.size}): " +
            s"${baseline.toSeq.sorted.take(20)}\n" +
            s"  withHrc  (HRC on,  size=${withHrc.size}):  " +
            s"${withHrc.toSeq.sorted.take(20)}")
      }
    }
  }

  test("HRC reuse-fired plan-shape invariants (P2a-5d sentinel #10)") {
    // P2a-5d regression sentinel for the reuse-broadcast invariant lifted
    // from P2a-5c F2.2 (stage5-code-review-P2a-5c.md). End-to-end checkAnswer
    // in RED #9 only proves answer parity, not reuse parity -- a TrueLiteral
    // fallback would also yield correct answers. This test asserts the
    // raison-d'etre of HRC: BHJ and HRC must SHARE the BroadcastExchange,
    // never plan a second one (silent M1-shape regression).
    //
    // Spike evidence /tmp/hrc-m2-p2a-5d-1-spike.log 2026-05-26 verbatim:
    //   ReusedExchangeExec count = 1
    //   HRCExec.plan.class = BroadcastedHashedRelationRef
    //   plan tree: BHJ build = BroadcastExchange [plan_id=82]
    //              HRC ref   = BroadcastExchange [plan_id=82]   (same id)
    //              probe pre-filter = ReusedExchange [plan_id=82]
    //
    // No production change paired with this test -- it codifies the invariant
    // already established by P2a-5c-r2 GREEN end-to-end. Future P2b (AQE) and
    // P2c (composite key) MUST keep it green.
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
        // Run to materialize ReusedExchange dedup (preparations run on access).
        df.collect()
        val exec = df.queryExecution.executedPlan

        // Invariant 1: HRCExec present and carries the expected subquery type.
        val hrcExecs = exec.flatMap { sp =>
          sp.expressions.flatMap(_.collect { case e: HashedRelationContainsExec => e })
        }
        assert(hrcExecs.nonEmpty,
          s"Expected at least one HashedRelationContainsExec.\nPlan:\n${exec.treeString}")
        // Accept bare ref OR ReusedSubqueryExec wrap (Gap D dispatch covers both).
        hrcExecs.foreach { e =>
          val isRef = e.plan.isInstanceOf[BroadcastedHashedRelationRef]
          val isReusedRef = e.plan match {
            case org.apache.spark.sql.execution.ReusedSubqueryExec(_: BroadcastedHashedRelationRef) =>
              true
            case _ => false
          }
          assert(isRef || isReusedRef,
            s"HRCExec.plan must be BroadcastedHashedRelationRef or " +
              s"ReusedSubqueryExec(BroadcastedHashedRelationRef); got ${e.plan.getClass.getName}")
        }

        // Invariant 2: exactly ONE BroadcastExchangeExec total across the whole
        // plan including subqueries -- BHJ and HRC must SHARE it. Two = silent
        // M1-shape regression (second broadcast materialization). Use
        // collectWithSubqueries to descend into the HRC subquery subtree.
        val allBroadcastExchanges = exec.collectWithSubqueries {
          case b: BroadcastExchangeExec => b
        }
        assert(allBroadcastExchanges.size == 1,
          s"HRC must reuse the BHJ BroadcastExchange. Found " +
            s"${allBroadcastExchanges.size} BroadcastExchangeExec instances " +
            s"(expected 1).\nPlan:\n${exec.treeString}")

        // Invariant 3: at least one ReusedExchangeExec -- proves dedup fired
        // (the second BroadcastExchange the rule planned was collapsed by
        // ReuseExchangeAndSubquery).
        val reused = exec.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
        assert(reused.nonEmpty,
          s"Expected at least one ReusedExchangeExec (reuse must fire).\n" +
            s"Plan:\n${exec.treeString}")
      }
    }
  }

  test("HashedRelationContainsExec does not mix in CodegenFallback (P2c-0 RED #11)") {
    // P2c-0 RED #11 (codegen mandate per todos
    // features/spark-hashed-relation-contains/docs/0008-investigation-p2c-0-codegen-design.md
    // rev 2 + stage2-design-review-r9). HashedRelationContainsExec currently
    // mixes in CodegenFallback (HashedRelationContainsExec.scala L61 + L50-51
    // self-batch "MVP scope: CodegenFallback path. First-class doGenCode is a
    // later optimization slice"). SPIP Q7 / impl-plan section 7 Open Q3 mandate
    // first-class doGenCode for PR #1; this test anchors the class-shape RED.
    //
    // Reflection-based anchor (most stable): CodegenFallback presence in the
    // linearization implies the per-row eval path (nullSafeEval wrapper). After
    // P2c-0 GREEN, the trait must be gone -- doGenCode is implemented inline,
    // mirroring BloomFilterMightContain.doGenCode + BHJ.prepareBroadcast.
    val klass = classOf[HashedRelationContainsExec]
    val fallbackTrait =
      classOf[org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback]
    val mixesIn = fallbackTrait.isAssignableFrom(klass)
    assert(!mixesIn,
      s"HashedRelationContainsExec must NOT mix in CodegenFallback after P2c-0 " +
        s"(per decision rev9 D7 codegen mandate). doGenCode must be implemented " +
        s"inline using ctx.addReferenceObj(broadcast) + relationTerm.getValue lookup " +
        s"(peer: BHJ.prepareBroadcast + BloomFilterMightContain.doGenCode). " +
        s"Current linearization includes ${fallbackTrait.getName}.")
  }

  test("HashedRelationContainsExec emits inline broadcast-ref codegen (P2c-0 RED #12)") {
    // P2c-0 RED #12: end-to-end behavioural anchor. After P2c-0 GREEN,
    // HashedRelationContainsExec.doGenCode injects the broadcast handle via
    // ctx.addReferenceObj("broadcast", this.broadcast), which produces a
    // generated Java snippet like:
    //   ((org.apache.spark.broadcast.Broadcast) references[$idx] /* broadcast */)
    // This is the HRC-SPECIFIC anchor that distinguishes HRC's broadcast-ref
    // codegen from BHJ's prepareBroadcast (BHJ uses
    // buildPlan.executeBroadcast() captured into its own broadcast variable;
    // the comment "/* broadcast */" appears in BHJ's generated source too, but
    // the surrounding hashedrelationcontains#NNN identifier is HRC-unique).
    //
    // RED while CodegenFallback is mixed in: HRC falls back to per-row Scala
    // eval(), no inline broadcast reference is emitted in the generated Java
    // -- the only broadcast reference present is BHJ's, not HRC's.
    import org.apache.spark.sql.execution.debug.codegenString
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
        df.collect()
        val codegenDump = codegenString(df.queryExecution.executedPlan)
        // Anchor: HRC.doGenCode emits a mutable state named "hrcRelation_N"
        // via ctx.addMutableState(HashedRelation.class.getName, "hrcRelation",
        // forceInline=true) -- this is HRC-unique (BHJ's prepareBroadcast uses
        // "relation_N" without the "hrc" prefix). CodegenFallback path emits
        // neither (HRC predicate hidden behind nullSafeEval).
        val hasHrcMutableState = codegenDump.contains("hrcRelation")
        assert(hasHrcMutableState,
          s"Expected `hrcRelation` mutable state in generated Java (proves HRC " +
            s"doGenCode inlined ctx.addMutableState(HashedRelation, \"hrcRelation\", " +
            s"forceInline=true) + relationTerm.getValue lookup; CodegenFallback " +
            s"hides the predicate behind nullSafeEval wrapper).\n" +
            s"Generated Java dump head (first 4000 chars):\n${codegenDump.take(4000)}")
      }
    }
  }

  test("Composite int+int join injects HRC (P2c-1 B.1 RED #13)") {
    // P2c-1 B.1 RED #13: composite (size > 1) equi-join with packed-Long path
    // (two IntegralType keys, sum <= 8B). Currently `size == 1` guard at
    // InjectHashedRelationFilters.scala L51 short-circuits; expect FAIL until
    // P2c-1 GREEN lifts the guard.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("build2", "probe2") {
        spark.range(8).selectExpr("cast(id as int) as k1", "cast(id as int) as k2")
          .createOrReplaceTempView("build2")
        spark.range(10000).selectExpr("cast(id as int) as k1", "cast(id as int) as k2")
          .createOrReplaceTempView("probe2")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build2) */ probe2.k1 FROM probe2 JOIN build2 ON " +
            "probe2.k1 = build2.k1 AND probe2.k2 = build2.k2")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.nonEmpty,
          s"Expected HRC injection on composite int+int join (packed-Long path), " +
            s"but found none.\nPlan:\n${optimized.treeString}")
      }
    }
  }

  test("Composite int+string join injects HRC via UnsafeRow fallback (P2c-1 B.2 RED #14)") {
    // P2c-1 B.2 RED #14: composite equi-join falling out of packed-Long path
    // (string forces UnsafeRow fallback per HashJoin.rewriteKeyExpr L743-747
    // canRewriteAsLongType check: not all IntegralType). Currently `size == 1`
    // guard rejects; expect FAIL until P2c-1 GREEN.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("build3", "probe3") {
        spark.range(8).selectExpr("cast(id as int) as k1", "cast(id as string) as k2")
          .createOrReplaceTempView("build3")
        spark.range(10000).selectExpr("cast(id as int) as k1", "cast(id as string) as k2")
          .createOrReplaceTempView("probe3")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build3) */ probe3.k1 FROM probe3 JOIN build3 ON " +
            "probe3.k1 = build3.k1 AND probe3.k2 = build3.k2")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.nonEmpty,
          s"Expected HRC injection on composite int+string join (UnsafeRow fallback), " +
            s"but found none.\nPlan:\n${optimized.treeString}")
      }
    }
  }
}
