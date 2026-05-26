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
import org.apache.spark.sql.execution.runtimefilter.{BroadcastedHashedRelationRef, HashedRelationContainsExec}
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
}
