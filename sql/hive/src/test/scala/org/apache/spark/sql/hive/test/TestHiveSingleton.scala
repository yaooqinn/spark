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

package org.apache.spark.sql.hive.test

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.{CommandProcessorFactory, CommandProcessorResponse}
import org.apache.hadoop.hive.ql.session.SessionState
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.client.{hive, HiveClient}


trait TestHiveSingleton extends SparkFunSuite with BeforeAndAfterAll {
  override protected val enableAutoThreadAudit = false
  protected val spark: SparkSession = TestHive.sparkSession
  protected val hiveContext: TestHiveContext = TestHive
  protected val hiveClient: HiveClient =
    spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

  protected override def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }

  def runSqlHive(sql: String): Seq[String] = {
    TestHiveSingleton.runSqlHive(sql, hiveClient)
  }
}

object TestHiveSingleton extends Logging {
  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   * Since upgrading the built-in Hive to 2.3, hive-llap-client is needed when
   * running MapReduce jobs with `runHive`.
   * Since HIVE-17626(Hive 3.0.0), need to set hive.query.reexecution.enabled=false.
   */
  protected def runHive(hiveClient: HiveClient, cmd: String, maxRows: Int = 1000): Seq[String] = {
    hiveClient.withHiveState {
      // Since HIVE-18238(Hive 3.0.0), the Driver.close function's return type changed
      // and the CommandProcessorFactory.clean function removed.
      val state = hiveClient.getState.asInstanceOf[SessionState]
      def closeDriver(driver: Driver): Unit = {
        driver.getClass.getMethod("close").invoke(driver)
        if (hiveClient.version != hive.v3_0 && hiveClient.version != hive.v3_1) {
          CommandProcessorFactory.clean(state.getConf)
        }
      }

      // Hive query needs to start SessionState.
      SessionState.start(state)
      logDebug(s"Running hiveql '$cmd'")
      if (cmd.toLowerCase(Locale.ROOT).startsWith("set")) { logDebug(s"Changing config: $cmd") }
      try {
        val cmd_trimmed: String = cmd.trim()
        val tokens: Array[String] = cmd_trimmed.split("\\s+")
        // The remainder of the command.
        val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
        val proc = CommandProcessorFactory.get(tokens, state.getConf)
        proc match {
          case driver: Driver =>
            val response: CommandProcessorResponse = driver.run(cmd)
            // Throw an exception if there is an error in query processing.
            if (response.getResponseCode != 0) {
              closeDriver(driver)
              throw new QueryExecutionException(response.getErrorMessage)
            }
            driver.setMaxRows(maxRows)

            val results = {
              val res = new java.util.ArrayList[Object]()
              driver.getResults(res)
              res.asScala.map {
                case s: String => s
                case a: Array[Object] => a(0).asInstanceOf[String]
              }.toSeq
            }
            closeDriver(driver)
            results

          case _ =>
            if (state.out != null) {
              // scalastyle:off println
              state.out.println(tokens(0) + " " + cmd_1)
              // scalastyle:on println
            }
            val response: CommandProcessorResponse = proc.run(cmd_1)
            // Throw an exception if there is an error in query processing.
            if (response.getResponseCode != 0) {
              throw new QueryExecutionException(response.getErrorMessage)
            }
            Seq(response.getResponseCode.toString)
        }
      } finally {
        if (state != null) {
          state.close()
        }
      }
    }
  }

  def runSqlHive(sql: String, client: HiveClient): Seq[String] = {
    val maxResults = 100000
    val results = runHive(client, sql, maxResults)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) throw SparkException.internalError("RESULTS POSSIBLY TRUNCATED")
    results
  }
}
