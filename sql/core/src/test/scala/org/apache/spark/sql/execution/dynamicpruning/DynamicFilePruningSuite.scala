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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{
  And, AttributeReference, DynamicPruningExpression, IsNotNull, Literal
}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.execution.{
  FileSourceScanExec, FilterExec, InSubqueryExec, ProjectExec, SubqueryBroadcastExec
}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, PushDpeToFileScan}
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
}
