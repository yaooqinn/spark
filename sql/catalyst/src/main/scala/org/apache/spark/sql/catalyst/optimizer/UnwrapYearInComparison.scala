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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.BINARY_COMPARISON
import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
 * Unwrap `year(date_col) <op> N` to a `BETWEEN`-shape predicate on `date_col`
 * itself, where N is an integer literal year and `<op>` is a binary comparison.
 *
 * The rewrite enables three optimizations simultaneously:
 *   1. Datasource pushdown: predicates on the raw date column reach the
 *      Parquet/JDBC scan (PushedFilters), while predicates on `year(date_col)`
 *      do not (year() is not a pushable expression).
 *   2. Codegen elimination: removes per-row `year()` UnaryExpression calls;
 *      Tungsten codegen path on date arithmetic is unblocked.
 *   3. CBO precision: column statistics min/max are precise on the raw date
 *      column, so selectivity estimation no longer goes through `year()`.
 *
 * Rewrite shapes (`d` is a `DateType` column, `N` is an integer literal year):
 * {{{
 *   year(d) =  N   ==>  d >= make_date(N,1,1)  AND  d <= make_date(N,12,31)
 *   year(d) <> N   ==>  d <  make_date(N,1,1)  OR   d >  make_date(N,12,31)
 *   year(d) >  N   ==>  d >  make_date(N,12,31)
 *   year(d) >= N   ==>  d >= make_date(N,1,1)
 *   year(d) <  N   ==>  d <  make_date(N,1,1)
 *   year(d) <= N   ==>  d <= make_date(N,12,31)
 * }}}
 *
 * In addition, the rule supports the symmetric form `N <op> year(d)` by flipping
 * the comparison.
 *
 * NULL semantics: `Year.nullIntolerant = true`, so `year(NULL) -> NULL`. The
 * BETWEEN-style rewrite also produces NULL on a NULL `d`. Both branches are
 * equivalent under NULL inputs.
 *
 * Validation gates (Phase 1 = DateType only):
 *   - `child.dataType == DateType` (always true because `Year` has
 *     `ImplicitCastInputTypes` with `Seq(DateType)`, so the analyzer has
 *     already cast or rejected non-DateType children at this point).
 *   - `n in [LocalDate.MIN.getYear .. LocalDate.MAX.getYear]` to ensure the
 *     constructed boundary `LocalDate.of(n, 1, 1)` / `LocalDate.of(n, 12, 31)`
 *     does not throw `DateTimeException`. Out-of-range years are NOT rewritten;
 *     the original expression remains and evaluates as before.
 *
 * Out of Phase 1 scope (deferred to backlog):
 *   - `TimestampType` / `TimestampNTZType` columns (timezone semantics needed).
 *   - `year(d) IN (y1, y2, ...)` (combinatorial plan growth; needs cost analysis).
 *   - Sibling rules for `month()` / `quarter()` / `dayofweek()`.
 *
 * Trino direct port template: `UnwrapYearInComparison.java` (~250 LoC) provides
 * the production-validated reference; this rule is a direct Scala port adapted
 * to Spark's `BinaryComparison` ADT and `Literal(Date)` representation.
 *
 * Spark precedent: SPARK-32706 `UnwrapCastInBinaryComparison` (same Optimizer
 * batch position, same Literal-side rewrite pattern, same NULL handling shape).
 */
object UnwrapYearInComparison extends Rule[LogicalPlan] {

  // LocalDate's valid year range. Spark Date columns can in principle store any
  // value the encoder accepts (epoch days as Int), but the BETWEEN literals we
  // construct go through LocalDate.of(year, month, day), which only accepts
  // years in [-999999999, 999999999]. We use this range as the guard so that
  // the constructed boundary dates never throw DateTimeException.
  private val minYear: Int = LocalDate.MIN.getYear
  private val maxYear: Int = LocalDate.MAX.getYear

  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressionsWithPruning(_.containsPattern(BINARY_COMPARISON), ruleId) {
      // Canonical orientation: year(d) <op> Literal(year)
      case EqualTo(Year(d), IntegerLiteral(n)) if validYear(n) =>
        And(GreaterThanOrEqual(d, firstDayOf(n)), LessThanOrEqual(d, lastDayOf(n)))
      case Not(EqualTo(Year(d), IntegerLiteral(n))) if validYear(n) =>
        Or(LessThan(d, firstDayOf(n)), GreaterThan(d, lastDayOf(n)))
      case GreaterThan(Year(d), IntegerLiteral(n)) if validYear(n) =>
        GreaterThan(d, lastDayOf(n))
      case GreaterThanOrEqual(Year(d), IntegerLiteral(n)) if validYear(n) =>
        GreaterThanOrEqual(d, firstDayOf(n))
      case LessThan(Year(d), IntegerLiteral(n)) if validYear(n) =>
        LessThan(d, firstDayOf(n))
      case LessThanOrEqual(Year(d), IntegerLiteral(n)) if validYear(n) =>
        LessThanOrEqual(d, lastDayOf(n))

      // Symmetric orientation: Literal(year) <op> year(d) -- flip comparison.
      case EqualTo(IntegerLiteral(n), Year(d)) if validYear(n) =>
        And(GreaterThanOrEqual(d, firstDayOf(n)), LessThanOrEqual(d, lastDayOf(n)))
      case GreaterThan(IntegerLiteral(n), Year(d)) if validYear(n) =>
        LessThan(d, firstDayOf(n))
      case GreaterThanOrEqual(IntegerLiteral(n), Year(d)) if validYear(n) =>
        LessThanOrEqual(d, lastDayOf(n))
      case LessThan(IntegerLiteral(n), Year(d)) if validYear(n) =>
        GreaterThan(d, lastDayOf(n))
      case LessThanOrEqual(IntegerLiteral(n), Year(d)) if validYear(n) =>
        GreaterThanOrEqual(d, firstDayOf(n))
    }

  private def validYear(n: Int): Boolean = n >= minYear && n <= maxYear

  private def firstDayOf(year: Int): Literal = dateLit(year, 1, 1)
  private def lastDayOf(year: Int): Literal = dateLit(year, 12, 31)

  private def dateLit(year: Int, month: Int, day: Int): Literal = {
    val days = DateTimeUtils.localDateToDays(LocalDate.of(year, month, day))
    Literal.create(days, org.apache.spark.sql.types.DateType)
  }
}
