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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SPARK-44662 V1 Dynamic File Pruning — P1 implementation tests.
 *
 * P1a (this batch): SQLConf registration only. Subsequent batches:
 *   - P1b: rule injection
 *   - P1b-2: footer-level file prune
 *   - P1d: R1-R10 risk coverage
 */
class BroadcastFilePruningSuite extends QueryTest with SharedSparkSession {

  test("P1a — DYNAMIC_FILE_PRUNING_ENABLED conf registered and defaults to false") {
    val value = spark.conf.get(SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key)
    assert(value == "false",
      s"Expected default 'false' for ${SQLConf.DYNAMIC_FILE_PRUNING_ENABLED.key}, got '$value'")
  }
}
