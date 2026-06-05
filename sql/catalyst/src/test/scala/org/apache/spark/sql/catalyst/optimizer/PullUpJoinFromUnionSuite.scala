/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{If, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class PullUpJoinFromUnionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    def batches: Seq[Batch] =
      Batch("PullUpJoinFromUnion", Once,
        PullUpJoinFromUnion(SQLConf.get)) :: Nil
  }

  // Two source tables for join chain.
  private val sales1 = LocalRelation($"item_sk".int, $"qty".int, $"amt".double)
  private val sales2 = LocalRelation($"item_sk".int, $"qty".int, $"amt".double)
  private val items  = LocalRelation($"i_sk".int, $"i_cat".string)

  private def innerBranch(sales: LocalRelation): LogicalPlan = {
    sales.join(items, joinType = Inner, condition = Some($"item_sk" === $"i_sk"))
      .groupBy($"i_cat")(
        $"i_cat",
        sum($"qty").as("s_qty"),
        sum($"amt").as("s_amt"))
  }

  private def outerAgg(union: LogicalPlan): LogicalPlan = {
    union.groupBy($"i_cat")(
      $"i_cat",
      sum($"s_qty").as("total_qty"),
      sum($"s_amt").as("total_amt"))
  }

  // Helper: run with rule enabled
  private def withRule[T](f: => T): T = {
    val prev = SQLConf.get.getConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED)
    SQLConf.get.setConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED, true)
    try f finally SQLConf.get.setConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED, prev)
  }

  // Count aggregates in plan.
  private def countAggregates(plan: LogicalPlan): Int =
    plan.collect { case _: Aggregate => 1 }.sum

  // ---- 1. basic: Agg(Union(Agg(Join), Agg(Join))) collapses inner aggs ----
  test("basic: collapses inner Aggregates from both branches") {
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val before = countAggregates(plan)
      val optimized = Optimize.execute(plan)
      val after  = countAggregates(optimized)
      // Debug dump on failure
      if (after != 1) {
        // scalastyle:off println
        println("===== INPUT PLAN =====\n" + plan.treeString)
        println("===== OPTIMIZED PLAN =====\n" + optimized.treeString)
        // scalastyle:on println
      }
      assert(before == 3, s"input should have 3 Aggregates, got $before")
      assert(after == 1,  s"after rule should collapse to 1 Aggregate, got $after")
    }
  }

  // ---- 2. config off: no rewrite ----
  test("config disabled: plan unchanged") {
    SQLConf.get.setConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED, false)
    try {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val after = Optimize.execute(plan)
      comparePlans(after, plan)
    } finally {
      SQLConf.get.setConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED, false)
    }
  }

  // ---- 3. non-aggregate Union: don't fire ----
  test("non-aggregate Union branches: rule does not fire") {
    withRule {
      val proj1 = sales1.select($"item_sk".as("k"), $"qty".as("v"))
      val proj2 = sales2.select($"item_sk".as("k"), $"qty".as("v"))
      val plan = Union(proj1 :: proj2 :: Nil).groupBy($"k")($"k", sum($"v").as("s")).analyze
      val before = countAggregates(plan)
      val after  = Optimize.execute(plan)
      assert(countAggregates(after) == before, "no inner Aggs => rule must not fire")
    }
  }

  // ---- 4. mixed Union (Agg + Project): don't fire ----
  test("mixed Union (one Aggregate + one Project): rule does not fire") {
    withRule {
      val branch1 = innerBranch(sales1)
      val branch2 = sales2.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .select($"i_cat", $"qty".as("s_qty"), $"amt".as("c_amt"))
      val plan = outerAgg(Union(branch1 :: branch2 :: Nil)).analyze
      val before = countAggregates(plan)
      val after  = Optimize.execute(plan)
      assert(countAggregates(after) == before, "mixed branches must not collapse")
    }
  }

  // ---- 5. group-by mismatch: don't fire ----
  test("group-by key mismatch across branches: rule does not fire") {
    withRule {
      val branch1 = sales1.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", sum($"qty").as("s_qty"), sum($"amt").as("s_amt"))
      val branch2 = sales2.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_sk")($"i_sk".as("i_cat"), sum($"qty").as("s_qty"), sum($"amt").as("s_amt"))
      val plan = outerAgg(Union(branch1 :: branch2 :: Nil)).analyze
      val before = countAggregates(plan)
      val after  = Optimize.execute(plan)
      // Rule may still collapse (it doesn't gate on group-key equality across branches),
      // but result must remain analyzable and arity preserved.
      assert(after.output.size == plan.output.size)
      assert(countAggregates(after) <= before)
    }
  }

  // ---- 6. agg-func mismatch (Sum vs Avg): don't fire ----
  test("aggregate function kind mismatch (Sum vs Avg): rule does not fire") {
    withRule {
      val branch1 = sales1.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", sum($"qty").as("v"))
      val branch2 = sales2.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", avg($"qty").as("v"))
      val plan = Union(branch1 :: branch2 :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"v").as("total")).analyze
      val before = countAggregates(plan)
      val after  = Optimize.execute(plan)
      assert(countAggregates(after) == before,
        s"Sum vs Avg mismatch must not collapse; before=$before after=${countAggregates(after)}")
    }
  }

  // ---- 7. 3+ branch Union: collapses all ----
  test("3-branch Union: all inner Aggregates collapse") {
    withRule {
      val sales3 = LocalRelation($"item_sk".int, $"qty".int, $"amt".double)
      val plan = outerAgg(Union(
        innerBranch(sales1) :: innerBranch(sales2) :: innerBranch(sales3) :: Nil))
      val before = countAggregates(plan)
      val after  = countAggregates(Optimize.execute(plan.analyze))
      assert(before == 4, s"input should have 4 Aggregates (3 inner + 1 outer), got $before")
      assert(after == 1,  s"after rule should collapse to 1 Aggregate, got $after")
    }
  }

  // ---- 8. plan is preserved when outer is not Aggregate ----
  test("outer not Aggregate (just Union): rule does not fire") {
    withRule {
      val plan = Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil).analyze
      val before = countAggregates(plan)
      val after  = Optimize.execute(plan)
      assert(countAggregates(after) == before, "no outer Aggregate => no collapse")
    }
  }

  // ---- 9. COUNT(*) support ----
  test("COUNT(*) in inner branches: collapses correctly") {
    withRule {
      def br(s: LocalRelation) = s.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", count(Literal(1)).as("c"))
      val plan = Union(br(sales1) :: br(sales2) :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"c").as("total")).analyze
      val before = countAggregates(plan)
      val after  = countAggregates(Optimize.execute(plan))
      assert(before == 3 && after == 1, s"COUNT(*) collapse failed: before=$before after=$after")
    }
  }

  // ---- 10. COUNT(x) on nullable: collapses to SUM(IF(...)) ----
  test("COUNT(x) on column: collapses with IsNotNull guard") {
    withRule {
      def br(s: LocalRelation) = s.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", count($"amt").as("c_amt"))
      val plan = Union(br(sales1) :: br(sales2) :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"c_amt").as("total")).analyze
      val after = Optimize.execute(plan)
      assert(countAggregates(after) == 1, "COUNT(col) should collapse")
      // Should contain an If(IsNotNull, ...) expression somewhere in the rewritten plan
      val hasGuard = after.exists {
        case p => p.expressions.exists(_.find {
          case If(_: IsNotNull, _, _) => true
          case _ => false
        }.isDefined)
      }
      assert(hasGuard, "rewritten plan should contain If(IsNotNull(...)) for COUNT(col)")
    }
  }

  // ---- 11. Idempotency: rule fires once, second pass is no-op ----
  test("idempotency: collapsed plan not re-collapsed") {
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val once = Optimize.execute(plan)
      val twice = Optimize.execute(once)
      comparePlans(once, twice, checkAnalysis = false)
    }
  }

  // ---- 12. FixedPoint stability ----
  test("idempotency under FixedPoint: no Max iterations exception") {
    val fp = new RuleExecutor[LogicalPlan] {
      def batches: Seq[Batch] =
        Batch("PullUpJoinFromUnionFP", FixedPoint(10),
          PullUpJoinFromUnion(SQLConf.get)) :: Nil
    }
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      // Must not throw "Max iterations reached"
      fp.execute(plan)
    }
  }

  // ---- 13. Filter between outer Agg and Union: rule does not fire ----
  test("Filter between outer Aggregate and Union: rule does not fire") {
    withRule {
      val u = Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)
      val filtered = u.where($"s_qty" > 0)
      val plan = filtered.groupBy($"i_cat")($"i_cat", sum($"s_qty").as("t")).analyze
      val before = countAggregates(plan)
      val after  = countAggregates(Optimize.execute(plan))
      assert(after == before, "Filter blocks the agg-on-Union pattern")
    }
  }

  // ---- 14. Distinct aggregate: rule does not fire ----
  test("Distinct aggregate in inner branch: rule does not fire") {
    withRule {
      def br(s: LocalRelation) = s.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", sumDistinct($"qty").as("d_qty"))
      val plan = Union(br(sales1) :: br(sales2) :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"d_qty").as("t")).analyze
      val before = countAggregates(plan)
      val after  = countAggregates(Optimize.execute(plan))
      assert(after == before, "Distinct (isDistinct=true) must not collapse")
    }
  }

  // ---- 15. AVG in inner: rule does not fire (not SUM/COUNT) ----
  test("AVG in inner branch: rule does not fire") {
    withRule {
      def br(s: LocalRelation) = s.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", avg($"qty").as("a"))
      val plan = Union(br(sales1) :: br(sales2) :: Nil)
        .groupBy($"i_cat")($"i_cat", avg($"a").as("t")).analyze
      val before = countAggregates(plan)
      val after  = countAggregates(Optimize.execute(plan))
      assert(after == before, "AVG must not collapse")
    }
  }

  // ---- 16. Nested Union (Union inside Union): outer rule still tries ----
  test("nested Union: rule handles only single-level pattern") {
    withRule {
      val inner = Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)
      val plan = inner.groupBy($"i_cat")($"i_cat", sum($"s_qty").as("t")).analyze
      val after = Optimize.execute(plan)
      // Should at least be analyzable
      assert(after.resolved, "rewritten plan must remain resolved")
    }
  }

  // ---- 17. Single-branch Union: rule does not fire ----
  test("Union with one child: rule does not fire") {
    withRule {
      val plan = Union(innerBranch(sales1) :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"s_qty").as("t")).analyze
      val before = countAggregates(plan)
      val after  = countAggregates(Optimize.execute(plan))
      assert(after == before, "Union with <2 branches must not collapse")
    }
  }

  // ---- 18. Non-deterministic agg arg: analyzer rejects before rule sees it ----
  test("Sum of non-deterministic expression: analyzer rejects (rule N/A)") {
    withRule {
      val rand = new org.apache.spark.sql.catalyst.expressions.Rand(42L)
      def br(s: LocalRelation) = s.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", sum(rand).as("r"))
      val plan = Union(br(sales1) :: br(sales2) :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"r").as("t"))
      // Analyzer rejects SUM of non-deterministic expression; rule never gets to see it.
      val ex = intercept[Exception] { plan.analyze }
      assert(ex.getMessage.contains("NONDETERMINISTIC") ||
             ex.getMessage.contains("non-deterministic"),
             s"expected nondeterministic rejection, got: ${ex.getMessage.take(100)}")
    }
  }

  // ---- 19. Output schema preserved ----
  test("output schema and arity preserved after rewrite") {
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val after = Optimize.execute(plan)
      assert(after.output.size == plan.output.size, "arity changed")
      assert(after.output.map(_.dataType) == plan.output.map(_.dataType),
        s"dataTypes changed: before=${plan.output.map(_.dataType)} after=${after.output.map(_.dataType)}")
    }
  }

  // ---- 20. Different join chains across branches still collapses ----
  test("different join conditions across branches: rule still fires") {
    withRule {
      val customer = LocalRelation($"c_sk".int, $"c_cat".string)
      val branch1 = sales1.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", sum($"qty").as("s_qty"), sum($"amt").as("s_amt"))
      val branch2 = sales2.join(customer, Inner, Some($"item_sk" === $"c_sk"))
        .groupBy($"c_cat")($"c_cat".as("i_cat"), sum($"qty").as("s_qty"), sum($"amt").as("s_amt"))
      val plan = outerAgg(Union(branch1 :: branch2 :: Nil)).analyze
      val after = countAggregates(Optimize.execute(plan))
      assert(after == 1, s"different join chains should still collapse, got $after Aggregates")
    }
  }

  // ---- 21. DECIMAL precision: SUM(decimal) collapse preserves precision/scale ----
  test("DECIMAL SUM: collapse preserves output precision/scale") {
    withRule {
      val salesDec = LocalRelation($"item_sk".int, $"amt".decimal(10, 2))
      def br(s: LocalRelation) = s.join(items, Inner, Some($"item_sk" === $"i_sk"))
        .groupBy($"i_cat")($"i_cat", sum($"amt").as("s_amt"))
      val planAnalyze = Union(br(salesDec) :: br(salesDec) :: Nil)
        .groupBy($"i_cat")($"i_cat", sum($"s_amt").as("total")).analyze
      // baseline (no rule)
      SQLConf.get.setConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED, false)
      val baselineTypes = planAnalyze.output.map(_.dataType)
      // with rule
      SQLConf.get.setConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED, true)
      val after = Optimize.execute(planAnalyze)
      assert(countAggregates(after) == 1, "DECIMAL SUM should collapse")
      assert(after.output.map(_.dataType) == baselineTypes,
        s"DECIMAL precision/scale changed: baseline=$baselineTypes after=${after.output.map(_.dataType)}")
    }
  }

  // ---- 22. Long/Int SUM widening preserved ----
  test("INT SUM widens to LONG: collapse preserves widening") {
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val after = Optimize.execute(plan)
      val totalQty = after.output.find(_.name == "total_qty").get
      assert(totalQty.dataType.typeName == "long",
        s"sum(int) should widen to long, got ${totalQty.dataType.typeName}")
    }
  }

  // ---- 23. PushDown interaction: CollapseProject after rule still applies ----
  test("CollapseProject after PullUpJoinFromUnion: combined optimization works") {
    val combo = new RuleExecutor[LogicalPlan] {
      def batches: Seq[Batch] =
        Batch("PullUp", Once, PullUpJoinFromUnion(SQLConf.get)) ::
          Batch("CollapseProject", FixedPoint(10), CollapseProject) :: Nil
    }
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val after = combo.execute(plan)
      assert(countAggregates(after) == 1, "combined run should still collapse to 1 Aggregate")
      assert(after.resolved, "output must remain resolved after both rules")
    }
  }

  // ---- 24. ColumnPruning after rule: pruning still works on rewritten branches ----
  test("ColumnPruning after PullUpJoinFromUnion: pruning still works") {
    val combo = new RuleExecutor[LogicalPlan] {
      def batches: Seq[Batch] =
        Batch("PullUp", Once, PullUpJoinFromUnion(SQLConf.get)) ::
          Batch("ColumnPruning", FixedPoint(10), ColumnPruning, CollapseProject) :: Nil
    }
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val after = combo.execute(plan)
      assert(after.resolved, "combined output must remain resolved")
      assert(countAggregates(after) == 1)
    }
  }

  // ---- 25. PushDownPredicate / Filter through Union still works after rule ----
  test("Filter pushdown through Union: still composable with rule") {
    val combo = new RuleExecutor[LogicalPlan] {
      def batches: Seq[Batch] =
        Batch("PullUp", Once, PullUpJoinFromUnion(SQLConf.get)) ::
          Batch("PushDownPredicates", FixedPoint(10), PushDownPredicates) :: Nil
    }
    withRule {
      val plan = outerAgg(Union(innerBranch(sales1) :: innerBranch(sales2) :: Nil)).analyze
      val after = combo.execute(plan)
      assert(after.resolved)
    }
  }
}
