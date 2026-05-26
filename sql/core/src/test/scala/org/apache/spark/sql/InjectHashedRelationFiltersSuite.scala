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

import org.apache.spark.sql.catalyst.optimizer.InjectHashedRelationFilters
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for the new HRC injection rule (SPARK-XXXXX, HRC PR #1, Core MVP).
 *
 * Tracks the first RED slice of P2a per todos
 * features/spark-hashed-relation-contains/docs/0003-implementation-plan.md rev 8.
 * Subsequent RED tests (BHJ-injects-HRC behavioral checks) land alongside the
 * rule implementation. This file exists primarily to anchor the compile-time
 * RED that proves the rule object does not yet exist.
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
}
