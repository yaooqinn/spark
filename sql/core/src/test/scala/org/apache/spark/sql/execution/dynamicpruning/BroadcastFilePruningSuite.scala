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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SPARK-44662 V1 Dynamic File Pruning — P1 implementation tests.
 *
 * P1a: SQLConf registration only.
 * P1b (added in this commit): rule injects DynamicPruningExpression on
 *      BHJ over Parquet data column.
 * Subsequent batches:
 *   - P1b-2: footer-level file prune
 *   - P1d: R1-R10 risk coverage
 */
class BroadcastFilePruningSuite extends QueryTest with SharedSparkSession {

  test("P1a — DYNAMIC_FILE_PRUNING_ENABLED conf registered and defaults to false") {
    val value = spark.conf.get(SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key)
    assert(value == "false",
      s"Expected default 'false' for ${SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key}, got '$value'")
  }

  test("P1b — rule injects DynamicPruningExpression on BHJ over Parquet data col (non-AQE)") {
    withSQLConf(
      SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000000") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(1000).selectExpr("id as fact_key", "id * 2 as v")
          .write.mode("overwrite").parquet(s"$path/fact")
        spark.range(100, 105).selectExpr("id as dim_key")
          .write.mode("overwrite").parquet(s"$path/dim")

        val fact = spark.read.parquet(s"$path/fact")
        val dim = spark.read.parquet(s"$path/dim")
        val df = fact.join(dim, fact("fact_key") === dim("dim_key"))
          .selectExpr("fact_key", "v")
        // P1b scope: assert rule injection only; execution correctness
        // (Parquet footer skip) is P1b-2. Don't collect() here.
        val plan = df.queryExecution.executedPlan
        val bhj = plan.collectFirst { case b: BroadcastHashJoinExec => b }
        assert(bhj.isDefined, s"Expected BroadcastHashJoinExec in plan: $plan")
        val streamSide = bhj.get.buildSide match {
          case BuildLeft => bhj.get.right
          case BuildRight => bhj.get.left
        }
        val factScan = streamSide.collectFirst { case s: FileSourceScanExec => s }
        assert(factScan.isDefined, s"Expected FileSourceScanExec in stream side: $streamSide")
        val hasDPE = factScan.get.dataFilters
          .exists(_.getClass.getSimpleName == "DynamicPruningExpression")
        assert(hasDPE,
          s"Expected DynamicPruningExpression in scan.dataFilters when DFP enabled, " +
            s"got: ${factScan.get.dataFilters}")
      }
    }
  }

  test("P1b-2 — DFP skips Parquet files whose footer min/max exclude all build keys") {
    withSQLConf(
      SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10000000",
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1024") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        // Write fact as 4 separate files with disjoint key ranges:
        // file 0: 0-249, file 1: 250-499, file 2: 500-749, file 3: 750-999.
        // Each file gets its own footer min/max bracket.
        (0 until 4).foreach { i =>
          val lo = i * 250
          val hi = lo + 250
          spark.range(lo, hi).selectExpr("id as fact_key", "id * 2 as v")
            .coalesce(1)
            .write.mode("append").parquet(s"$path/fact")
        }
        // dim only contains keys in file 1's bracket [250, 499].
        spark.range(300, 305).selectExpr("id as dim_key")
          .write.mode("overwrite").parquet(s"$path/dim")

        val fact = spark.read.parquet(s"$path/fact")
        val dim = spark.read.parquet(s"$path/dim")
        val df = fact.join(dim, fact("fact_key") === dim("dim_key"))
          .selectExpr("fact_key", "v")
        df.collect()

        val plan = df.queryExecution.executedPlan
        val factScan = plan.collectFirst { case s: FileSourceScanExec
          if s.relation.location.rootPaths.head.toString.contains("/fact") => s }.get
        // numFiles metric should reflect files surviving DFP pruning.
        // Without DFP: all 4 files read. With DFP: only file 1 (keys 250-499) survives.
        val numFilesMetric = factScan.metrics("numFiles").value
        assert(numFilesMetric < 4,
          s"Expected DFP to skip at least one of 4 fact files (build keys 300-304 " +
            s"only intersect 1 file), but numFiles=$numFilesMetric")
      }
    }
  }
}

