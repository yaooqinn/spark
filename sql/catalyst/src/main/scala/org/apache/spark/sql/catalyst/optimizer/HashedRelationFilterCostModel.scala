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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Cost model gating HashedRelationContains injection. Per-rule-invocation pure
 * decisions + caller-managed per-scan budget Map (filtersInjectedPerScan). All
 * formulas are size-based (4 SQLConf AND-gates, no selectivity); selectivity
 * and cost-vs-benefit ranking are deferred pending benchmark evidence (see JIRA
 * SPARK-XXXXX, sections "Q2-bis SPIP 6.2 deviation" and "Q3-bis SPIP 6.3
 * deviation").
 *
 * Thread-safety: this object holds no mutable state. The budget Map is owned by
 * the caller (`InjectHashedRelationFilters.apply`), constructed fresh per call,
 * not shared across concurrent queries.
 *
 * Budget key anchor (per-scan): `output.head.exprId.id: Long` of the probe-side
 * leaf scan. ExprId is stable across `transformWithPruning` (verified via
 * spike, see JIRA SPARK-XXXXX); peer-parity with `InjectRuntimeFilter`
 * `hasBloomFilter` fastEquals-on-AttributeReference; self-join semantics
 * aligned with the ExprId-strict Bloom mutex.
 */
private[sql] object HashedRelationFilterCostModel {

  sealed trait Decision { def reason: String }
  final case class Inject(reason: String) extends Decision
  final case class Skip(reason: String) extends Decision

  /**
   * D.0 skeleton -- returns a placeholder Skip pending D.1-D.5 gate-wiring batches.
   * Unit suite asserts the placeholder shape; integration suite (Gate D in
   * InjectHashedRelationFiltersSuite) will assert real gating once D.1+ wires
   * the SQLConfs into the rule body.
   */
  def shouldInject(
      buildPlan: LogicalPlan,
      probePlan: LogicalPlan,
      probeScanAnchor: Long,
      budget: mutable.Map[Long, Int],
      hasBloomOnSameLineage: Boolean,
      conf: SQLConf): Decision = {
    Skip("d0-skeleton-not-yet-wired")
  }

  /**
   * Rank candidate builds by size ascending (smaller broadcast = cheaper). Used
   * when a single probe scan has multiple candidate HRC injections competing
   * for the `maxFiltersPerScan` budget.
   *
   * Note: SPIP section 6.2 proposes a selectivity formula
   * `1 - buildSize/domainCardinality`, which requires `ColumnStat.distinctCount`
   * (frequently None per SPARK-21043 / SPARK-30269); P2d ships size-based as
   * the always-available surrogate.
   */
  def rankBuilds(builds: Seq[LogicalPlan]): Seq[LogicalPlan] = {
    builds.sortBy(_.stats.sizeInBytes.toLong)
  }
}
