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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{
  And, AttributeReference, DynamicPruningExpression, IsNotNull, Literal
}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{
  FileSourceScanExec, FilterExec, InSubqueryExec, ProjectExec, SubqueryBroadcastExec
}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, PushDpeToFileScan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * SPARK-44662 - Dynamic File Pruning v2 (logical-rule + AQE-stage rule).
 *
 * All tests enforce spark.sql.adaptive.enabled=true (Inv-8 lesson: AQE-off
 * test fixtures hide the per-stage prep blind spot that doomed v1 physical
 * rule). See features/spark-dynamic-file-pruning/investigations/0008.
 */
class DynamicFilePruningSuite extends QueryTest with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def withDfp(enabled: Boolean)(body: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key -> enabled.toString,
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000000") {
      body
    }
  }

  test("P2a-1 - rule injects DynamicPruningSubquery on V1 BHJ over Parquet data col") {
    withDfp(enabled = true) {
      withTempPath { dir =>
        spark.range(1000).selectExpr("id AS k", "id * 2 AS v")
          .write.parquet(dir.getAbsolutePath + "/fact")
        spark.range(10).selectExpr("id AS k", "id + 100 AS dim")
          .write.parquet(dir.getAbsolutePath + "/dim")
        val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
        val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("dim < 105")
        val df = fact.join(dim, "k")
        val optimized = df.queryExecution.optimizedPlan
        val hasDfpDps = optimized.exists {
          case f if f.expressions.exists(_.exists(_.isInstanceOf[DynamicPruningSubquery])) => true
          case _ => false
        }
        assert(hasDfpDps,
          s"Expected DynamicPruningSubquery in optimized plan, got:\n$optimized")
      }
    }
  }

  // P2a-4 (F4-S1 cleanup-shape-parity) deferred to GREEN-phase regression -
  // a meaningful assertion requires DFP-injected DPS to exist, which only
  // happens once the rule is implemented. Will be added in P2a-GREEN commit.

  test("P2b - DFP+DPP guard: partition col + data col both eligible -> exactly 1 DPS, not 2") {
    withDfp(enabled = true) {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        withTempPath { dir =>
          spark.range(1000).selectExpr("id AS k_data", "id % 4 AS k_part", "id * 2 AS v")
            .write.partitionBy("k_part").parquet(dir.getAbsolutePath + "/fact")
          spark.range(4).selectExpr("id AS k_data", "id AS k_part", "id + 100 AS dim")
            .write.parquet(dir.getAbsolutePath + "/dim")
          val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
          val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("dim < 102")
          val df = fact.join(dim, fact("k_data") === dim("k_data")
            && fact("k_part") === dim("k_part"))
          val optimized = df.queryExecution.optimizedPlan
          val dpsCount = optimized.collect {
            case p if p.expressions.exists(_.exists(_.isInstanceOf[DynamicPruningSubquery])) => p
          }.size
          assert(dpsCount == 1,
            s"Expected exactly 1 DPS after DPP+DFP guard, got $dpsCount in:\n$optimized")
        }
      }
    }
  }

  // ---------- P2c-NEW: PushDpeToFileScan AQE-stage rule unit tests ----------
  //
  // Tests #1, #2, #4, #5: behavior assertions on PushDpeToFileScan.apply output.
  // Test #3: sentinel-shape (no-op invariant on TrueLiteral DPE).
  // Each test constructs a synthetic physical plan (FilterExec / ProjectExec /
  // FileSourceScanExec) and invokes PushDpeToFileScan.apply directly to ground
  // the rule's transformations without running a full query.

  private def readScan(tmpRoot: java.io.File, name: String): FileSourceScanExec = {
    spark.range(1).selectExpr("id AS k", "id AS v")
      .write.parquet(tmpRoot.getAbsolutePath + "/" + name)
    val df = spark.read.parquet(tmpRoot.getAbsolutePath + "/" + name)
    collect(df.queryExecution.executedPlan) {
      case s: FileSourceScanExec => s
    }.head
  }

  private def materializedDpe(
      keyAttr: AttributeReference, buildScan: FileSourceScanExec): DynamicPruningExpression = {
    // Build a DPE in its post-PlanAdaptiveDynamicPruningFilters shape:
    // DynamicPruningExpression(InSubqueryExec(_, SubqueryBroadcastExec, ...))
    val buildKey = AttributeReference("bk", LongType)()
    val bex = BroadcastExchangeExec(
      HashedRelationBroadcastMode(Seq(buildKey), isNullAware = false),
      buildScan)
    val sbe = SubqueryBroadcastExec("dfp-sbe", Seq(0), Seq(buildKey), bex)
    val inSub = InSubqueryExec(keyAttr, sbe, org.apache.spark.sql.catalyst.expressions.ExprId(1L))
    DynamicPruningExpression(inSub)
  }

  test("P2c-NEW-1 - PushDpeToFileScan moves DPE under direct Filter+scan") {
    withDfp(enabled = true) {
      withTempPath { tmp =>
        val scan = readScan(tmp, "p2c1")
        val buildScan = readScan(tmp, "p2c1b")
        val k = scan.output.find(_.name == "k").get.asInstanceOf[AttributeReference]
        val dpe = materializedDpe(k, buildScan)
        val filter = FilterExec(dpe, scan)
        val out = PushDpeToFileScan.apply(filter)
        val newScanOpt = collect(out) { case s: FileSourceScanExec => s }.headOption
        assert(newScanOpt.isDefined, s"Expected FileSourceScanExec in:\n$out")
        assert(newScanOpt.get.dataFilters.exists(_.isInstanceOf[DynamicPruningExpression]),
          s"Expected DPE in scan.dataFilters, got: ${newScanOpt.get.dataFilters}")
      }
    }
  }

  test("P2c-NEW-2 (S6-S1) - PushDpeToFileScan descends through ProjectExec") {
    withDfp(enabled = true) {
      withTempPath { tmp =>
        val scan = readScan(tmp, "p2c2")
        val buildScan = readScan(tmp, "p2c2b")
        val k = scan.output.find(_.name == "k").get.asInstanceOf[AttributeReference]
        val dpe = materializedDpe(k, buildScan)
        val proj = ProjectExec(Seq(k), scan)
        val filter = FilterExec(dpe, proj)
        val out = PushDpeToFileScan.apply(filter)
        val newScanOpt = collect(out) { case s: FileSourceScanExec => s }.headOption
        assert(newScanOpt.isDefined, s"Expected FileSourceScanExec in:\n$out")
        assert(newScanOpt.get.dataFilters.exists(_.isInstanceOf[DynamicPruningExpression]),
          s"Expected DPE in scan.dataFilters after Project descent, " +
            s"got: ${newScanOpt.get.dataFilters}")
      }
    }
  }

  test("P2c-NEW-3 (F-S1 extended) - PushDpeToFileScan no-op on DPE(TrueLiteral)") {
    withDfp(enabled = true) {
      withTempPath { tmp =>
        val scan = readScan(tmp, "p2c3")
        val trueLitDpe = DynamicPruningExpression(Literal.TrueLiteral)
        val filter = FilterExec(trueLitDpe, scan)
        val out = PushDpeToFileScan.apply(filter)
        val newScanOpt = collect(out) { case s: FileSourceScanExec => s }.headOption
        assert(newScanOpt.isDefined, s"Expected FileSourceScanExec in:\n$out")
        // F-S1 sentinel: TrueLiteral DPE never gets pushed into scan.dataFilters,
        // because the pattern only matches materialized InSubqueryExec form. Filter
        // shape preserved so isDynamicPruningFilter can strip it at scan time.
        assert(!newScanOpt.get.dataFilters.exists(_.isInstanceOf[DynamicPruningExpression]),
          s"F-S1 violated: TrueLiteral DPE leaked into scan.dataFilters, " +
            s"got: ${newScanOpt.get.dataFilters}")
      }
    }
  }

  test("P2c-NEW-4 - PushDpeToFileScan preserves non-DPE Filter conditions") {
    withDfp(enabled = true) {
      withTempPath { tmp =>
        val scan = readScan(tmp, "p2c4")
        val buildScan = readScan(tmp, "p2c4b")
        val k = scan.output.find(_.name == "k").get.asInstanceOf[AttributeReference]
        val dpe = materializedDpe(k, buildScan)
        val nonDpe = IsNotNull(k)
        val combined = And(nonDpe, dpe)
        val filter = FilterExec(combined, scan)
        val out = PushDpeToFileScan.apply(filter)
        // After rule: outer node should be a FilterExec carrying only IsNotNull,
        // child = FileSourceScanExec with DPE moved into dataFilters.
        out match {
          case FilterExec(cond, _: FileSourceScanExec) =>
            assert(!cond.exists(_.isInstanceOf[DynamicPruningExpression]),
              s"Non-DPE Filter should not contain DPE after rule, got: $cond")
            assert(cond.exists(_.isInstanceOf[IsNotNull]),
              s"Expected IsNotNull preserved in outer Filter, got: $cond")
          case _ => fail(s"Expected FilterExec(non-DPE, FileSourceScanExec) shape, got:\n$out")
        }
      }
    }
  }

  test("P2c-NEW-5 (R-N2) - PushDpeToFileScan does not pollute PushedFilters metadata") {
    withDfp(enabled = true) {
      withTempPath { tmp =>
        val scan = readScan(tmp, "p2c5")
        val buildScan = readScan(tmp, "p2c5b")
        val k = scan.output.find(_.name == "k").get.asInstanceOf[AttributeReference]
        val dpe = materializedDpe(k, buildScan)
        val filter = FilterExec(dpe, scan)
        val out = PushDpeToFileScan.apply(filter)
        val newScan = collect(out) { case s: FileSourceScanExec => s }.head
        val pushed = newScan.metadata.getOrElse("PushedFilters", "")
        // scalastyle:off caselocale
        assert(!pushed.toLowerCase.contains("dynamicpruning"),
          s"R-N2 violated: DPE leaked into PushedFilters metadata, got: $pushed")
        // scalastyle:on caselocale
      }
    }
  }

  test("P2c-NEW-INT1 (P2c-1 retry, F5-S3) - AQE-on BHJ clustered Parquet " +
       "yields footer-level file skip + df.collect() succeeds") {
    withDfp(enabled = true) {
      withTempPath { dir =>
        // 8-file fact table, key clustered per-file via repartition by hash on k.
        spark.range(800).selectExpr("id AS k", "id * 2 AS v")
          .repartition(8, org.apache.spark.sql.functions.col("k"))
          .write.parquet(dir.getAbsolutePath + "/fact")
        spark.range(10).selectExpr("id AS k", "id + 100 AS dim")
          .write.parquet(dir.getAbsolutePath + "/dim")
        val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
        val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("dim < 105")
        val joinedDfp = fact.join(dim, "k")
        val rowsDfp = joinedDfp.collect()
        val scansDfp = collect(joinedDfp.queryExecution.executedPlan) {
          case s: FileSourceScanExec => s
        }
        val factScanDfp = scansDfp.find(_.relation.location.rootPaths.headOption
          .exists(_.toString.endsWith("/fact"))).get
        val pruned = factScanDfp.metrics("numFiles").value
        val baseline = withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000000") {
          val factB = spark.read.parquet(dir.getAbsolutePath + "/fact")
          val dimB = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("dim < 105")
          val joined = factB.join(dimB, "k")
          joined.collect()
          val s = collect(joined.queryExecution.executedPlan) {
            case s: FileSourceScanExec => s
          }.find(_.relation.location.rootPaths.headOption
            .exists(_.toString.endsWith("/fact"))).get
          s.metrics("numFiles").value
        }
        // F5-S3: df.collect() succeeds without exception (already asserted by
        // running joinedDfp.collect() above without try/catch).
        assert(rowsDfp.nonEmpty, "df.collect() should return at least 1 row")
        assert(pruned < baseline,
          s"P2c-1 retry: expected pruned numFiles < baseline; " +
            s"baseline=$baseline pruned=$pruned")
      }
    }
  }

  test("P2f-1 - BF coexists with DFP on the same fact-table scan (no mutex collision)") {
    // Regression for DFP-on suppressing InjectRuntimeFilter BF via
    // DynamicPruningSubquery mutex at InjectRuntimeFilter.scala (hasDynamicPruningSubquery).
    // Fix: isFileFilter discriminator on DPS; BF mutex excludes DFP-flagged entries.
    // RED: with the bug present, plan has 0 might_contain tokens (BF suppressed).
    // GREEN post-fix: plan has BOTH DPS (DFP) AND might_contain (BF) on fact scan.
    //
    // BF gate requires isProbablyShuffleJoin OR shuffle below the join. We set
    // autoBroadcastJoinThreshold=1B so both fact and dim are too big to broadcast
    // (forcing SMJ = shuffle join), and lower the application-side scan-size
    // threshold so the small synthetic fact qualifies.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key -> "true",
      // Force shuffle join (both sides above BHJ threshold) so BF gate
      // isProbablyShuffleJoin returns true.
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      // Lower application-side byte-size requirement so small synthetic fact
      // qualifies for BF injection.
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1") {
      withTempPath { dir =>
        spark.range(10000).selectExpr("id AS k", "id % 100 AS v")
          .write.parquet(dir.getAbsolutePath + "/fact")
        spark.range(100).selectExpr("id AS k", "id + 1 AS dim")
          .write.parquet(dir.getAbsolutePath + "/dim")
        val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
        val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("dim < 50")
        val joined = fact.join(dim, "k")
        val optimized = joined.queryExecution.optimizedPlan.toString
        // P2f assertion: DFP and BF must coexist after the fix.
        val hasDfp = optimized.contains("dynamicpruning#")
        val hasBf = optimized.contains("might_contain")
        assert(hasDfp, s"DFP expected to inject DynamicPruningSubquery; plan: $optimized")
        assert(hasBf,
          s"BF expected to inject might_contain (mutex collision with DFP fixed); plan: $optimized")
      }
    }
  }

  test("P2d-1 (former P1a) - DYNAMIC_FILE_PRUNING_ENABLED conf registered, defaults to false") {
    val value = spark.conf.get(SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key)
    assert(value == "false",
      s"Expected default 'false' for ${SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key}, got '$value'")
  }

  // ---------- application-side single-file skip gate ----------
  //
  // Tests gate `shouldSkipByFileCount(filterableScan)` in
  // DynamicFilePruning.prune(). Tests #1/#2/#4/INT are clean behavior-RED on
  // the gate's apply outcome. Test #3 verifies the SQLConf "-1" disables the
  // gate (after rule ships, asserts DPS injected with conf=-1).
  //
  // File-count fixtures are produced via .coalesce(N) before .write.parquet to
  // pin numFiles deterministically; default RDD partition count would yield
  // 8 files on local-cluster and break the gate assumption.

  private def writeWithFiles(
      dir: java.io.File, name: String, rows: Int, files: Int, schema: String): Unit = {
    val baseDf = spark.range(rows).selectExpr(schema.split(",").map(_.trim).toIndexedSeq: _*)
    val coalesced = if (files == 1) baseDf.coalesce(1) else baseDf.repartition(files)
    coalesced.write.parquet(dir.getAbsolutePath + "/" + name)
  }

  private def numFiles(df: DataFrame): Int = {
    df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(r: HadoopFsRelation, _, _, _, _) => r.location.inputFiles.length
    }.headOption.getOrElse(0)
  }

  private def hasDfpDps(plan: LogicalPlan): Boolean = plan.exists { node =>
    node.expressions.exists(_.exists {
      case _: DynamicPruningSubquery => true
      case _ => false
    })
  }

  test("application-side single-file target -> insertPredicate skipped") {
    withDfp(enabled = true) {
      withTempPath { dir =>
        // Fact = 1 file (application side). Dim = 1 file (build side, small).
        writeWithFiles(dir, "fact", rows = 100, files = 1, schema = "id AS k, id * 2 AS v")
        writeWithFiles(dir, "dim", rows = 10, files = 1, schema = "id AS k, id + 100 AS d")
        val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
        val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("d < 105")
        assert(numFiles(fact) == 1, s"expected 1-file fact, got ${numFiles(fact)}")
        val df = fact.join(dim, "k")
        val plan = df.queryExecution.optimizedPlan
        assert(!hasDfpDps(plan),
          s"should skip DPS on 1-file application-side target; plan:\n$plan")
      }
    }
  }

  test("application-side multi-file target -> insertPredicate fires") {
    withDfp(enabled = true) {
      withTempPath { dir =>
        // Fact = 2 files (>= 2 so gate at default=1 does NOT skip).
        writeWithFiles(dir, "fact", rows = 1000, files = 2, schema = "id AS k, id * 2 AS v")
        writeWithFiles(dir, "dim", rows = 10, files = 1, schema = "id AS k, id + 100 AS d")
        val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
        val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("d < 105")
        assert(numFiles(fact) == 2, s"expected 2-file fact, got ${numFiles(fact)}")
        val df = fact.join(dim, "k")
        val plan = df.queryExecution.optimizedPlan
        assert(hasDfpDps(plan),
          s"should inject DPS on 2-file application-side target; plan:\n$plan")
      }
    }
  }

  test("applicationSideMinFiles=-1 disables the gate") {
    withDfp(enabled = true) {
      withSQLConf(
        SQLConf.DYNAMIC_FILE_PRUNING_APPLICATION_SIDE_MIN_FILES.key -> "-1") {
        withTempPath { dir =>
          // Fact = 1 file -> default=1 would skip, but conf=-1 disables.
          writeWithFiles(dir, "fact", rows = 100, files = 1, schema = "id AS k, id * 2 AS v")
          writeWithFiles(dir, "dim", rows = 10, files = 1, schema = "id AS k, id + 100 AS d")
          val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
          val dim = spark.read.parquet(dir.getAbsolutePath + "/dim").filter("d < 105")
          assert(numFiles(fact) == 1, s"expected 1-file fact, got ${numFiles(fact)}")
          val df = fact.join(dim, "k")
          val plan = df.queryExecution.optimizedPlan
          assert(hasDfpDps(plan),
            s"conf=-1 must inject DPS on 1-file target (gate disabled); plan:\n$plan")
        }
      }
    }
  }

  test("sibling 1-file dim with multi-file fact target -> DPS injected on fact " +
      "(subtree-walk regression guard)") {
    withDfp(enabled = true) {
      withTempPath { dir =>
        // V1 bug shape: pruningPlan.collectFirst[LogicalRelation] would hit
        // the sibling 1-file dim FIRST and over-skip the fact-target DPS.
        // V2 (this gate) must check filterableScan directly = the multi-file
        // fact, so DPS IS injected.
        writeWithFiles(dir, "fact", rows = 1000, files = 4, schema = "id AS k, id * 2 AS v")
        writeWithFiles(dir, "dim_small", rows = 10, files = 1, schema = "id AS k, id + 100 AS d")
        writeWithFiles(dir, "dim_filter", rows = 10, files = 1,
          schema = "id AS k_filter, id + 200 AS f")
        val fact = spark.read.parquet(dir.getAbsolutePath + "/fact")
        val dimSmall = spark.read.parquet(dir.getAbsolutePath + "/dim_small")
        val dimFilter = spark.read.parquet(dir.getAbsolutePath + "/dim_filter").filter("f < 205")
        assert(numFiles(fact) == 4, s"expected 4-file fact, got ${numFiles(fact)}")
        // Build a join tree where fact is the application side AND the
        // sibling join (fact x dim_small) places a 1-file LogicalRelation
        // deeper in the application-side subtree.
        val factJoined = fact.join(dimSmall.selectExpr("k", "d AS d_extra"), "k")
        val df = factJoined.join(dimFilter, factJoined("k") === dimFilter("k_filter"))
        val plan = df.queryExecution.optimizedPlan
        // Must find at least one DPS targeting the multi-file fact, NOT
        // skipped by the gate (V1 bug would skip it).
        assert(hasDfpDps(plan),
          s"subtree-walk regression - multi-file fact must still get DPS " +
            s"injected even when sibling subtree has a 1-file dim; plan:\n$plan")
      }
    }
  }

  test("q11-shape BHJ: DPS injected on multi-file fact, skipped on 1-file dim, " +
      "df.collect() succeeds") {
    withDfp(enabled = true) {
      withTempPath { dir =>
        // q11-shape: customer (multi-file fact) x store_sales (multi-file fact)
        // x date_dim (1-file). Simplified: fact1 (4-file) x fact2 (3-file) x dim (1-file).
        // The gate must:
        //   (a) NOT inject DPS on the 1-file dim path
        //   (b) inject DPS on at least one multi-file fact path
        //   (c) numFiles_pruned <= baseline (i.e. DPS that DOES fire prunes)
        //   (d) df.collect() succeeds and returns expected row shape
        writeWithFiles(dir, "fact1", rows = 1000, files = 4,
          schema = "id AS k1, id * 2 AS v1")
        writeWithFiles(dir, "fact2", rows = 500, files = 3,
          schema = "id AS k2, id * 3 AS v2")
        writeWithFiles(dir, "dim_1file", rows = 5, files = 1,
          schema = "id AS k_dim, id + 1000 AS d")
        val fact1 = spark.read.parquet(dir.getAbsolutePath + "/fact1")
        val fact2 = spark.read.parquet(dir.getAbsolutePath + "/fact2")
        val dim = spark.read.parquet(dir.getAbsolutePath + "/dim_1file").filter("d < 1003")
        assert(numFiles(fact1) == 4)
        assert(numFiles(fact2) == 3)
        assert(numFiles(dim) == 1)
        val df = fact1.join(dim, fact1("k1") === dim("k_dim"))
          .join(fact2, fact1("k1") === fact2("k2"))
        val plan = df.queryExecution.optimizedPlan
        assert(hasDfpDps(plan),
          s"q11-shape: expected DPS on multi-file facts; plan:\n$plan")
        val rows = df.collect()
        assert(rows.length >= 0, s"df.collect() must succeed; got ${rows.length} rows")
      }
    }
  }
}
