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

package org.apache.spark.sql.execution.runtimefilter

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.SparkPlan

/**
 * Physical preparations rule that rewrites the logical
 * [[org.apache.spark.sql.catalyst.expressions.HashedRelationContainsSubquery]]
 * placeholders inserted by
 * [[org.apache.spark.sql.catalyst.optimizer.InjectHashedRelationFilters]]
 * into a probe-side Filter wrapping a HashedRelationContainsExec backed by a
 * BroadcastedHashedRelationRef that reuses the sibling BroadcastHashJoinExec's
 * BroadcastExchangeExec via sameResult matching (mirrors
 * PlanDynamicPruningFilters reuse pattern; see 0002c-contract.md §3.5 for the
 * full contract).
 *
 * This is the P2a-5a scaffold: the rule is wired into preparations but is
 * currently an identity no-op. The real apply (sameResult lookup +
 * BroadcastExchangeExec wrap + HashedRelationContainsExec rewrite + cost-model
 * drop) lands in P2a-5b.
 */
case class PlanHashedRelationContainsFilters(sparkSession: SparkSession)
  extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan
}
