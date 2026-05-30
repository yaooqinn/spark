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
}
