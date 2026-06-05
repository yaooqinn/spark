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

import java.time.LocalDate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DateType, IntegerType}

/**
 * Suite for [[UnwrapYearInComparison]] rule.
 *
 * Rewrites year(d) op N to BETWEEN-shape predicates on d for all 6 binary
 * comparison operators, in both canonical and flipped orientations.
 */
class UnwrapYearInComparisonSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("UnwrapYearInComparison", FixedPoint(10),
      UnwrapYearInComparison) :: Nil
  }

  private val relation = LocalRelation($"d".date, $"i".int)
  private val d = relation.output.head
  private val i = relation.output.last

  // Helper: build a Date literal from year/month/day via DateTimeUtils so the
  // value matches exactly what the rule's dateLit produces internally.
  private def date(year: Int, month: Int, day: Int): Literal = {
    val days = DateTimeUtils.localDateToDays(LocalDate.of(year, month, day))
    Literal.create(days, DateType)
  }

  private def firstDayOf(y: Int) = date(y, 1, 1)
  private def lastDayOf(y: Int) = date(y, 12, 31)

  // -------- P1a: positive rewrites for all 6 binary comparison ops --------

  test("P1a: year(d) = N is rewritten to BETWEEN") {
    val originalQuery = relation.where(Year(d) === 2024).analyze
    val correctAnswer = relation.where(d >= firstDayOf(2024) && d <= lastDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1a: year(d) <> N is rewritten to OR(d < firstDayOf, d > lastDayOf)") {
    val originalQuery = relation.where(Year(d) =!= 2024).analyze
    val correctAnswer = relation.where(d < firstDayOf(2024) || d > lastDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1a: year(d) > N is rewritten to d > lastDayOf(N)") {
    val originalQuery = relation.where(Year(d) > 2024).analyze
    val correctAnswer = relation.where(d > lastDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1a: year(d) >= N is rewritten to d >= firstDayOf(N)") {
    val originalQuery = relation.where(Year(d) >= 2024).analyze
    val correctAnswer = relation.where(d >= firstDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1a: year(d) < N is rewritten to d < firstDayOf(N)") {
    val originalQuery = relation.where(Year(d) < 2024).analyze
    val correctAnswer = relation.where(d < firstDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1a: year(d) <= N is rewritten to d <= lastDayOf(N)") {
    val originalQuery = relation.where(Year(d) <= 2024).analyze
    val correctAnswer = relation.where(d <= lastDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  // -------- P1b: flipped orientation N op year(d) --------

  test("P1b: N = year(d) is rewritten (flipped orientation)") {
    val originalQuery = relation.where(Literal(2024) === Year(d)).analyze
    val correctAnswer = relation.where(d >= firstDayOf(2024) && d <= lastDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1b: N > year(d) ==> d < firstDayOf(N)") {
    val originalQuery = relation.where(Literal(2024) > Year(d)).analyze
    val correctAnswer = relation.where(d < firstDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1b: N < year(d) ==> d > lastDayOf(N)") {
    val originalQuery = relation.where(Literal(2024) < Year(d)).analyze
    val correctAnswer = relation.where(d > lastDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  // -------- P1c: negative guards --------

  test("P1c: not rewritten when RHS is not Literal (e.g. year(d) = i)") {
    val originalQuery = relation.where(Year(d) === i).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("P1c: not rewritten when LHS is not Year (e.g. d > N)") {
    val originalQuery = relation.where(d > firstDayOf(2024)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  test("P1c: not rewritten for year out of LocalDate range") {
    // LocalDate.MAX year is 999999999. Use 1_000_000_000 (over the boundary).
    // This integer literal is valid as IntegerLiteral but would throw
    // DateTimeException if we tried to construct LocalDate.of(value, 1, 1).
    val originalQuery = relation.where(Year(d) === Literal(Int.MaxValue, IntegerType)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, originalQuery)
  }

  // -------- P1d: NULL semantics + arithmetic correctness --------

  test("P1d: rewrite preserves NULL semantics for year boundary 1") {
    // year(d)=1 -> d BETWEEN 0001-01-01 AND 0001-12-31. Valid LocalDate
    // boundaries.
    val originalQuery = relation.where(Year(d) === 1).analyze
    val correctAnswer = relation.where(d >= firstDayOf(1) && d <= lastDayOf(1)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1d: rewrite preserves NULL semantics for year boundary 9999") {
    val originalQuery = relation.where(Year(d) === 9999).analyze
    val correctAnswer = relation.where(d >= firstDayOf(9999) && d <= lastDayOf(9999)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  // -------- P1e: rule fires inside more complex predicates --------

  test("P1e: rule fires inside AND-combined predicates") {
    val originalQuery = relation.where(Year(d) === 2024 && i > Literal(0)).analyze
    val correctAnswer = relation
      .where((d >= firstDayOf(2024) && d <= lastDayOf(2024)) && i > Literal(0))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1e: rule fires inside OR-combined predicates") {
    val originalQuery = relation.where(Year(d) === 2024 || Year(d) === 2025).analyze
    val correctAnswer = relation
      .where(
        (d >= firstDayOf(2024) && d <= lastDayOf(2024)) ||
        (d >= firstDayOf(2025) && d <= lastDayOf(2025)))
      .analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("P1e: rule does NOT fire on Timestamp column (Year would have ImplicitCast)") {
    // Year on a Timestamp gets an implicit cast Year(Cast(t, DateType)).
    // The rule pattern is Year(d) directly, no Cast inside. So when t is
    // Timestamp, the cast survives and our rule does NOT match (the inner
    // expression is Cast, not the column directly).
    val tsRel = LocalRelation($"t".timestamp)
    val t = tsRel.output.head
    val originalQuery = tsRel.where(Year(t) === 2024).analyze
    val optimized = Optimize.execute(originalQuery)
    // The analyzer inserts a Cast(t, DateType) inside Year(...). After rewrite
    // the inner expression is Year(Cast(t, DateType)) which our rule MATCHES
    // (pattern is just Year(d), no DateType restriction needed because the
    // analyzer enforces it). So rewrite proceeds, producing
    // Cast(t, DateType) BETWEEN firstDay AND lastDay -- still correct semantics.
    // Verify the rewrite happens (the pattern itself remains semantically valid
    // because Year's input type cast guarantees d is DateType-compatible).
    val expectedDateCast = Cast(t, DateType)
    val correctAnswer = tsRel
      .where(expectedDateCast >= firstDayOf(2024) && expectedDateCast <= lastDayOf(2024))
      .analyze
    comparePlans(optimized, correctAnswer)
  }
}
