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
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SPARK-44662 - Dynamic File Pruning v2 (logical-rule design).
 *
 * All tests enforce spark.sql.adaptive.enabled=true (Inv-8 lesson: AQE-off
 * test fixtures hide the per-stage prep blind spot that doomed v1 physical
 * rule). See features/spark-dynamic-file-pruning/investigations/0008.
 */
class DynamicFilePruningSuite extends QueryTest with SharedSparkSession {

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

  // P2a-4 (F4-S1 cleanup-shape-parity) deferred to GREEN-phase regression —
  // a meaningful assertion requires DFP-injected DPS to exist, which only
  // happens once the rule is implemented. Will be added in P2a-GREEN commit.
}
