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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BloomFilterMightContain, EqualTo, HashedRelationContainsSubquery, Literal, XxHash64}
import org.apache.spark.sql.catalyst.optimizer.InjectHashedRelationFilters
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.runtimefilter.{BroadcastedHashedRelationRef, HashedRelationContainsExec, PlanHashedRelationContainsFilters}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, IntegerType}

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
class InjectHashedRelationFiltersSuite extends SharedSparkSession
  with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {

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

  test("Composite int+int correctness vs HRC off (P2c-1 B.3 + B.6 RED #15)") {
    // P2c-1 B.3 (correctness packed-Long, >=1k rows + collision-prone keys) +
    // B.6 (HRC actually filters; not BHJ-救回 invisible bug, per stage2-r10 F2.1).
    // Build = 16 keys, probe = 4000 rows where only ~25% have a matching build
    // key under the COMPOSITE (k1, k2) tuple. Compare HRC on vs off; if the
    // composite probe-key shape is bit-misaligned, packed-Long lookup misses
    // and BHJ救回 still yields the right answer -- so we also assert HRC node
    // is present in the executed plan (proof HRC participated).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("b15", "p15") {
        // Build: 16 composite keys (k1=0..3, k2=0..3 cartesian).
        spark.range(16).selectExpr(
          "cast(id % 4 as int) as k1",
          "cast(id / 4 as int) as k2",
          "id as v").createOrReplaceTempView("b15")
        // Probe: 10_000 rows; collision-prone (small int domain forces shared
        // hash buckets) + ~25% genuine matches under (k1, k2) AND semantics.
        // Must be > 5000-byte threshold so probe is NOT broadcastable
        // (otherwise InjectHashedRelationFilters early-returns).
        spark.range(10000).selectExpr(
          "cast(id % 16 as int) as k1",
          "cast(id % 8 as int) as k2",
          "id as v").createOrReplaceTempView("p15")
        val sqlStr =
          "SELECT /*+ BROADCAST(b15) */ p15.v FROM p15 JOIN b15 ON " +
            "p15.k1 = b15.k1 AND p15.k2 = b15.k2"
        val baseline = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
          spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        }
        val withHrc = spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        assert(withHrc == baseline,
          s"Composite int+int HRC on must equal HRC off.\n" +
            s"  baseline size=${baseline.size}, withHrc size=${withHrc.size}\n" +
            s"  baseline-withHrc=${(baseline -- withHrc).take(10)}\n" +
            s"  withHrc-baseline=${(withHrc -- baseline).take(10)}")
        // B.6 anchor: HRC node MUST be present in the executed plan (proves
        // the filter participated; without this, a bit-misalignment in the
        // packed-Long lookup would silent-miss and BHJ救回 still pass checkAnswer).
        // Materialize first so AQE finalizes (HRCExec lives in final plan, not
        // the pre-AQE SubqueryAdaptiveHRCExec placeholder).
        val df = spark.sql(sqlStr)
        df.collect()
        val hrcExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        assert(hrcExecs.nonEmpty,
          s"Expected HashedRelationContainsExec in executed plan (proves HRC " +
            s"participated; without this, BHJ救回 could mask a bit-misalignment " +
            s"bug).\nPlan:\n${df.queryExecution.executedPlan.treeString}")
      }
    }
  }

  test("Composite int+string correctness via UnsafeRow fallback (P2c-1 B.4 RED #16)") {
    // P2c-1 B.4 (correctness UnsafeRow fallback path, >=1k rows). String key
    // forces canRewriteAsLongType=false; rewriteKeyExpr returns the original
    // Seq; doGenCode emits GenerateUnsafeProjection.createCode and
    // HashedRelation.getValue(InternalRow) byte-compares against the build-
    // side UnsafeRow packing. Any schema/dataType mismatch yields 100% miss.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("b16", "p16") {
        spark.range(16).selectExpr(
          "cast(id % 4 as int) as k1",
          "cast(id / 4 as string) as k2",
          "id as v").createOrReplaceTempView("b16")
        spark.range(10000).selectExpr(
          "cast(id % 16 as int) as k1",
          "cast(id % 8 as string) as k2",
          "id as v").createOrReplaceTempView("p16")
        val sqlStr =
          "SELECT /*+ BROADCAST(b16) */ p16.v FROM p16 JOIN b16 ON " +
            "p16.k1 = b16.k1 AND p16.k2 = b16.k2"
        val baseline = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
          spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        }
        val withHrc = spark.sql(sqlStr).collect().map(_.getLong(0)).toSet
        assert(withHrc == baseline,
          s"UnsafeRow fallback HRC on must equal HRC off.\n" +
            s"  baseline size=${baseline.size}, withHrc size=${withHrc.size}\n" +
            s"  baseline-withHrc=${(baseline -- withHrc).take(10)}\n" +
            s"  withHrc-baseline=${(withHrc -- baseline).take(10)}")
        val df = spark.sql(sqlStr)
        df.collect()
        val hrcExecs = collectWithSubqueries(df.queryExecution.executedPlan) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        assert(hrcExecs.nonEmpty,
          s"Expected HashedRelationContainsExec on UnsafeRow fallback path.\n" +
            s"Plan:\n${df.queryExecution.executedPlan.treeString}")
      }
    }
  }

  test("Swapped-key composite ON-clause yields same answer (P2c-1 B.7 RED #17)") {
    // P2c-1 B.7 (per stage2-r10 F3.1): probe (k1=y AND k2=w) and (k2=w AND
    // k1=y) must produce identical row sets, AND both must equal the HRC-off
    // baseline. Sentinel for broadcastKeyIndices vs probe-key zipped order.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("b17", "p17") {
        spark.range(16).selectExpr(
          "cast(id % 4 as int) as k1",
          "cast(id / 4 as int) as k2",
          "id as v").createOrReplaceTempView("b17")
        spark.range(10000).selectExpr(
          "cast(id % 16 as int) as k1",
          "cast(id % 8 as int) as k2",
          "id as v").createOrReplaceTempView("p17")
        val orderA =
          "SELECT /*+ BROADCAST(b17) */ p17.v FROM p17 JOIN b17 ON " +
            "p17.k1 = b17.k1 AND p17.k2 = b17.k2"
        val orderB =
          "SELECT /*+ BROADCAST(b17) */ p17.v FROM p17 JOIN b17 ON " +
            "p17.k2 = b17.k2 AND p17.k1 = b17.k1"
        val baseline = withSQLConf(
          SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "false") {
          spark.sql(orderA).collect().map(_.getLong(0)).toSet
        }
        val dfA = spark.sql(orderA)
        val dfB = spark.sql(orderB)
        val withHrcA = dfA.collect().map(_.getLong(0)).toSet
        val withHrcB = dfB.collect().map(_.getLong(0)).toSet
        assert(withHrcA == baseline && withHrcB == baseline,
          s"Swapped composite key order must yield same answer as HRC off.\n" +
            s"  baseline=${baseline.size}, A=${withHrcA.size}, B=${withHrcB.size}")
        // Anchor: HRC node MUST be present on both orderings (proves the
        // swapped-key ON-clause both hit composite HRC inject, not just
        // false-green via 0-inject identical baseline).
        val hrcA = collectWithSubqueries(dfA.queryExecution.executedPlan) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        val hrcB = collectWithSubqueries(dfB.queryExecution.executedPlan) {
          case sp => sp.expressions.flatMap(_.collect {
            case e: HashedRelationContainsExec => e
          })
        }.flatten
        assert(hrcA.nonEmpty && hrcB.nonEmpty,
          s"Expected HRCExec on both orderings (A=${hrcA.size}, B=${hrcB.size})")
      }
    }
  }

  // ===========================================================================
  // Unit tests for `InjectHashedRelationFilters.hasBloomOnSameScanLineage`.
  // End-to-end coverage is constrained by `InjectRuntimeFilter`'s heuristic,
  // which declines Bloom injection in the BHJ-on-top shape that would also
  // trigger HRC injection, so we exercise the helper in isolation on small
  // hand-built `LogicalPlan`s. The `RUNTIME_BLOOM_FILTER_ENABLED=false` cases
  // are end-to-end baseline checks that HRC still injects when Bloom is off.
  // ===========================================================================

  /** Build a `BloomFilterMightContain` over `key` using a placeholder binary
   * scalar. The helper only inspects `bf.right.references`; the bloom binary
   * is irrelevant. */
  private def bloomMightContain(key: Attribute): BloomFilterMightContain =
    BloomFilterMightContain(
      Literal(null, BinaryType),
      new XxHash64(Seq(key)))

  test("hasBloomOnSameScanLineage defers when Bloom shares scan lineage " +
    "(single key)") {
    val c1 = AttributeReference("c1", IntegerType)()
    val c2 = AttributeReference("c2", IntegerType)()
    val scanX = LocalRelation(c1, c2)
    val probePlan: LogicalPlan = Filter(bloomMightContain(c1), scanX)

    assert(
      InjectHashedRelationFilters.hasBloomOnSameScanLineage(probePlan, Seq(c1)),
      "Bloom on c1 and HRC probe key c1 share the same scan lineage; " +
        "helper must defer HRC inject")
  }

  test("hasBloomOnSameScanLineage does not defer when Bloom is on an " +
    "unrelated scan") {
    val x1 = AttributeReference("x1", IntegerType)()
    val x2 = AttributeReference("x2", IntegerType)()
    val y1 = AttributeReference("y1", IntegerType)()
    val y2 = AttributeReference("y2", IntegerType)()
    val scanX = LocalRelation(x1, x2)
    val scanY = LocalRelation(y1, y2)
    val join = Join(scanX, scanY, Inner, Some(EqualTo(x1, y1)), JoinHint.NONE)
    val probePlan: LogicalPlan = Filter(bloomMightContain(y1), join)

    assert(
      !InjectHashedRelationFilters.hasBloomOnSameScanLineage(probePlan, Seq(x1)),
      "Bloom on scan_Y and HRC probe key on scan_X are unrelated; " +
        "helper must not defer HRC inject")
  }

  test("hasBloomOnSameScanLineage respects ExprId strictness across " +
    "Alias rename") {
    val c1 = AttributeReference("c1", IntegerType)()
    val c2 = AttributeReference("c2", IntegerType)()
    val scanX = LocalRelation(c1, c2)
    val renamed = Alias(c1, "renamed")()
    val proj = Project(Seq(renamed), scanX)
    val probePlan: LogicalPlan = Filter(bloomMightContain(renamed.toAttribute), proj)

    assert(
      !InjectHashedRelationFilters.hasBloomOnSameScanLineage(probePlan, Seq(c1)),
      "Bloom key is the Alias output (fresh ExprId), HRC key is the original " +
        "attribute; ExprId-strict equality must not consider them the same lineage")
  }

  test("hasBloomOnSameScanLineage any-matches across composite HRC keys") {
    val c1 = AttributeReference("c1", IntegerType)()
    val c2 = AttributeReference("c2", IntegerType)()
    val scanX = LocalRelation(c1, c2)
    val probePlan: LogicalPlan = Filter(bloomMightContain(c1), scanX)

    assert(
      InjectHashedRelationFilters.hasBloomOnSameScanLineage(probePlan, Seq(c1, c2)),
      "Composite HRC keys (c1, c2) with Bloom on c1; any-match must defer")
  }

  test("HRC still injects when Bloom filter is disabled (single key)") {
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("build_s1", "probe_s1") {
        spark.range(8).toDF("k").createOrReplaceTempView("build_s1")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe_s1")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build_s1) */ probe_s1.k FROM probe_s1 JOIN build_s1 " +
            "ON probe_s1.k = build_s1.k")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.nonEmpty,
          s"With Bloom disabled, the single-key HRC inject must still happen.\n" +
            s"Plan:\n${optimized.treeString}")
      }
    }
  }

  test("HRC still injects when Bloom filter is disabled (composite key)") {
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      withTempView("build_s2", "probe_s2") {
        spark.range(8).selectExpr("cast(id as int) as k1", "cast(id as int) as k2")
          .createOrReplaceTempView("build_s2")
        spark.range(10000).selectExpr("cast(id as int) as k1", "cast(id as int) as k2")
          .createOrReplaceTempView("probe_s2")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build_s2) */ probe_s2.k1 FROM probe_s2 JOIN build_s2 " +
            "ON probe_s2.k1 = build_s2.k1 AND probe_s2.k2 = build_s2.k2")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.nonEmpty,
          s"With Bloom disabled, the composite-key HRC inject must still happen.\n" +
            s"Plan:\n${optimized.treeString}")
      }
    }
  }

  test("hasBloomOnSameScanLineage short-circuits when mutual-exclusion " +
    "conf is false") {
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_BLOOM_MUTUAL_EXCLUSION.key -> "false") {
      val c1 = AttributeReference("c1", IntegerType)()
      val c2 = AttributeReference("c2", IntegerType)()
      val scanX = LocalRelation(c1, c2)
      val probePlan: LogicalPlan = Filter(bloomMightContain(c1), scanX)

      assert(
        !InjectHashedRelationFilters.hasBloomOnSameScanLineage(probePlan, Seq(c1)),
        "With the mutual-exclusion conf off, the helper must return false " +
          "regardless of plan shape")
    }
  }

  // ===========================================================================
  // P2c-3 Gate C — correctness audit (SPIP §2 (b)-(f) sentinels + joinType gate)
  // Design: todos features/spark-hashed-relation-contains/docs/
  //   0012-investigation-p2c-3-correctness-audit-design.md rev 2
  // Plan:   todos features/spark-hashed-relation-contains/docs/
  //   0003-implementation-plan.md rev 15 P2c-3 Gate C
  // ===========================================================================

  test("HRC does not inject when broadcast joins are disabled (P2c-3 C.1 SPIP (b))") {
    // SPIP (b): when the join cannot be a BroadcastHashJoin (e.g. all broadcast
    // joins disabled via AUTO_BROADCASTJOIN_THRESHOLD=-1), the HRC rule must
    // not inject. Detection short-circuits inside maybeInjectProbe because
    // canBroadcastBySize(buildPlan, conf) returns false for any plan when the
    // threshold is -1. This sentinel is GREEN-on-HEAD; it locks in the
    // current behavior so a later refactor cannot regress (b).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT probe.k FROM probe JOIN build ON probe.k = build.k")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.isEmpty,
          s"With broadcast joins disabled (threshold=-1), the HRC rule must not " +
            s"inject any HashedRelationContainsSubquery, but found ${injected.size}." +
            s"\nPlan:\n${optimized.treeString}")
        // Vacuous-pass guard: confirm the executed plan actually used SortMergeJoin,
        // proving the SPIP (b) precondition (no BHJ available) was real, not an
        // accidental no-join shape that trivially has 0 HRC injects.
        val executed = df.queryExecution.executedPlan
        val smj = executed.collect {
          case s: org.apache.spark.sql.execution.joins.SortMergeJoinExec => s
        }
        assert(smj.nonEmpty,
          s"SPIP (b) sentinel precondition not met: expected SortMergeJoinExec in " +
            s"the executed plan (broadcast disabled), but none found. Without an " +
            s"actual join, the 0-HRC-inject assertion above is vacuous.\nPlan:\n" +
            s"${executed.treeString}")
      }
    }
  }

  // C.2 SPIP (c) tiny probe -> not injected.
  // pending-p2d: HRC has a registered SQLConf
  // RUNTIME_HASHED_RELATION_CONTAINS_MIN_APPLICATION_SIZE
  // (accessor: runtimeFilterHashedRelationContainsMinApplicationSize), but
  // grep against InjectHashedRelationFilters.scala HEAD 6a79b4a0c4b shows 0
  // hits for the accessor or the conf key inside the rule body. The rule
  // gates injection on canBroadcastBySize for build/probe and on
  // runtimeFilterHashedRelationContainsEnabled / Bloom mutual-exclusion only;
  // probe stats are NOT consulted today. Authoring this test as `test(...)`
  // would either (a) FAIL because HRC still injects on a non-broadcastable
  // probe regardless of row count (real-but-deferred RED), or (b) PASS for
  // the wrong reason if the probe happens to be broadcastable too (vacuous
  // C.5 overlap). Both shapes hide the truth.
  //
  // Honest disposition (0012-investigation-p2c-3-correctness-audit-design.md
  // rev3 §2 / §2.1 + plan rev16 P2c-3 Gate C C.2 row): mark as `ignore` so
  // the slot is reserved but the suite count is unchanged. Activate (flip
  // `ignore` -> `test`) in P2d after MinApplicationSize is wired into
  // InjectHashedRelationFilters.apply / maybeInjectProbe. The body below is
  // the intended GREEN-after-P2d shape, kept here so reactivation is a
  // one-keyword edit.
  ignore("HRC does not inject when probe is below MinApplicationSize " +
    "(P2c-3 C.2 SPIP (c)) [pending-p2d: MinApplicationSize not wired]") {
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      // Force the probe NOT broadcastable on its own so the C.5 early-return
      // at maybeInjectProbe:L105 cannot mask the intended SPIP (c) check.
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        // 10 rows: well below any reasonable MIN_APPLICATION_SIZE; once P2d
        // wires the threshold, this probe should be deemed too small to be
        // worth filtering, so HRC must NOT inject.
        spark.range(10).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build ON probe.k = build.k")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.isEmpty,
          s"With probe row count below MinApplicationSize, the HRC rule must " +
            s"not inject, but found ${injected.size}.\nPlan:\n${optimized.treeString}")
      }
    }
  }

  test("HRC does not inject when build is too big to broadcast (P2c-3 C.3 SPIP (d))") {
    // SPIP (d): when the build side exceeds AUTO_BROADCASTJOIN_THRESHOLD, no
    // BHJ is planned, so HRC must not inject. Detection short-circuits at
    // maybeInjectProbe:L104 via canBroadcastBySize(buildPlan, conf) returning
    // false. GREEN-on-HEAD sentinel; vacuous-pass guard confirms the executed
    // plan really used SortMergeJoinExec (not an unexpected BHJ that would
    // make the 0-HRC-inject assertion trivial).
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      // Small threshold so range(1_000_000) cannot be broadcast as a build.
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1024",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("big_build", "big_probe") {
        // No BROADCAST hint: stats-based decision must reject both sides.
        spark.range(1000000).toDF("k").createOrReplaceTempView("big_build")
        spark.range(2000000).toDF("k").createOrReplaceTempView("big_probe")
        val df = spark.sql(
          "SELECT big_probe.k FROM big_probe JOIN big_build ON big_probe.k = big_build.k")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.isEmpty,
          s"With both sides above AUTO_BROADCASTJOIN_THRESHOLD, the HRC rule " +
            s"must not inject, but found ${injected.size}.\nPlan:\n" +
            s"${optimized.treeString}")
        val executed = df.queryExecution.executedPlan
        val smj = executed.collect {
          case s: org.apache.spark.sql.execution.joins.SortMergeJoinExec => s
        }
        assert(smj.nonEmpty,
          s"SPIP (d) sentinel precondition not met: expected SortMergeJoinExec " +
            s"in the executed plan (build too big to broadcast), but none found. " +
            s"Without an actual non-BHJ join, the 0-HRC-inject assertion above " +
            s"is vacuous.\nPlan:\n${executed.treeString}")
      }
    }
  }

  test("HRC does not inject on non-equi join (P2c-3 C.4 SPIP (e))") {
    // SPIP (e): the rule pattern-matches via ExtractEquiJoinKeys
    // (patterns.scala:L187+) which returns None when the join condition
    // contains no equality predicate. A non-equi join (e.g. probe.k <
    // build.k) therefore never enters maybeInjectProbe at all. GREEN-on-HEAD
    // sentinel; vacuous-pass guard asserts an actual non-equi join node
    // exists in the executed plan (BroadcastNestedLoopJoinExec when one side
    // is broadcastable) so the 0-HRC-inject assertion isn't trivially true
    // because no join was planned.
    withSQLConf(
      SQLConf.RUNTIME_HASHED_RELATION_CONTAINS_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "5000",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempView("build", "probe") {
        spark.range(8).toDF("k").createOrReplaceTempView("build")
        spark.range(10000).toDF("k").createOrReplaceTempView("probe")
        val df = spark.sql(
          "SELECT /*+ BROADCAST(build) */ probe.k FROM probe JOIN build " +
            "ON probe.k < build.k")
        val optimized = df.queryExecution.optimizedPlan
        val injected = optimized.collect {
          case f: Filter if f.condition.find(_.isInstanceOf[HashedRelationContainsSubquery])
            .isDefined => f
        }
        assert(injected.isEmpty,
          s"On a non-equi join (probe.k < build.k) the HRC rule must not " +
            s"inject (ExtractEquiJoinKeys non-match), but found ${injected.size}." +
            s"\nPlan:\n${optimized.treeString}")
        // Vacuous-pass guard: confirm a real join actually exists in the
        // executed plan. With BROADCAST(build), Spark plans non-equi joins
        // as BroadcastNestedLoopJoinExec.
        val executed = df.queryExecution.executedPlan
        val bnlj = executed.collect {
          case b: org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec => b
        }
        assert(bnlj.nonEmpty,
          s"SPIP (e) sentinel precondition not met: expected " +
            s"BroadcastNestedLoopJoinExec in the executed plan (non-equi join " +
            s"with BROADCAST hint), but none found. Without an actual " +
            s"non-equi join, the 0-HRC-inject assertion above is vacuous.\n" +
            s"Plan:\n${executed.treeString}")
      }
    }
  }
}
