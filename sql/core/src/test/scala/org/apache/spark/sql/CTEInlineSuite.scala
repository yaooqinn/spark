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

import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, GreaterThan, LessThan, Literal, Or, Rand}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class CTEInlineSuiteBase
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("SPARK-36447: non-deterministic CTE dedup") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() from t
           |)
           |select * from v except select * from v
         """.stripMargin)
      checkAnswer(df, Nil)

      val r = df.queryExecution.optimizedPlan.find {
        case RepartitionByExpression(p, _, None, _) => p.isEmpty
        case _ => false
      }
      assert(
        r.isDefined,
        "Non-deterministic With-CTE with multiple references should be not inlined.")
    }
  }

  test("SPARK-36447: non-deterministic CTE in subquery") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() c3 from t
           |)
           |select * from v where c3 not in (select c3 from v)
         """.stripMargin)
      checkAnswer(df, Nil)
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "Non-deterministic With-CTE with multiple references should be not inlined.")
    }
  }

  test("SPARK-36447: non-deterministic CTE with one reference should be inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() c3 from t
           |)
           |select c1, c2 from v where c3 > 0
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.exists(_.isInstanceOf[WithCTE]),
        "With-CTE should not be inlined in analyzed plan.")
      assert(
        !df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "With-CTE with one reference should be inlined in optimized plan.")
    }
  }

  test("SPARK-36447: nested non-deterministic CTEs referenced more than once are not inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select c1, c2, rand() c4 from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.length == 6,
        "With-CTE should contain 2 CTE def after optimization.")
    }
  }

  test("SPARK-36447: nested CTEs only the deterministic is inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.length == 4,
        "One CTE def should be inlined after optimization.")
    }
  }

  test("SPARK-36447: nested non-deterministic CTEs referenced only once are inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select c1, c2, c3, rand() c4 from v1
           |)
           |select c1, c2 from v2 where c3 > 0 and c4 > 0
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collectFirst {
          case r: RepartitionOperation => r
        }.isEmpty,
        "CTEs with one reference should all be inlined after optimization.")
    }
  }

  test("SPARK-36447: With in subquery of main query") {
    withSQLConf(
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      withTempView("t") {
        Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
        val df = sql(
          s"""with v as (
             |  select c1, c2, rand() c3 from t
             |)
             |select * from v except
             |select * from v where c1 = (
             |  with v2 as (
             |    select c1, c2, rand() c3 from t
             |  )
             |  select count(*) from v where c2 not in (
             |    select c2 from v2 where c3 not in (select c3 from v2)
             |  )
             |)
           """.stripMargin)
        checkAnswer(df, Nil)
        assert(
          collectWithSubqueries(df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }.length == 3,
          "Non-deterministic CTEs are reused shuffles.")
      }
    }
  }

  test("SPARK-36447: With in subquery of CTE def") {
    withTempView("t") {
      Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with v as (
           |  select c1, c2, rand() c3 from t where c1 = (
           |    with v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select count(*) from (
           |      select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |    )
           |  )
           |)
           |select count(*) from (
           |  select * from v where c1 > 0 union select * from v where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "Non-deterministic CTEs are reused shuffles.")
    }
  }

  test("SPARK-36447: nested deterministic CTEs are inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, c1 + c2 c3 from t
           |),
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collectFirst {
          case r: RepartitionOperation => r
        }.isEmpty,
        "Deterministic CTEs should all be inlined after optimization.")
    }
  }

  test("SPARK-36447: invalid nested CTEs") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val ex = intercept[AnalysisException](sql(
        s"""with
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |),
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin))
      checkErrorTableNotFound(ex, "`v1`",
        ExpectedContext("v1", 29, 30))
    }
  }

  test("CTE Predicate push-down and column pruning") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, 's' c3, rand() c4 from t
           |),
           |vv as (
           |  select v1.c1, v1.c2, rand() c5 from v v1, v v2
           |  where v1.c1 > 0 and v1.c3 = 's' and v1.c2 = v2.c2
           |)
           |select vv1.c1, vv1.c2, vv2.c1, vv2.c2 from vv vv1, vv vv2
           |where vv1.c2 > 0 and vv2.c2 > 0 and vv1.c1 = vv2.c1
         """.stripMargin)
      checkAnswer(df, Row(1, 2, 1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.length == 6,
        "CTE should not be inlined after optimization.")
      val distinctCteRepartitions = cteRepartitions.map(_.canonicalized).distinct
      // Check column pruning and predicate push-down.
      assert(distinctCteRepartitions.length == 2)
      assert(distinctCteRepartitions(1).collectFirst {
        case p: Project if p.projectList.length == 3 => p
      }.isDefined, "CTE columns should be pruned.")
      assert(distinctCteRepartitions(1).collectFirst {
        case f: Filter if f.condition.semanticEquals(GreaterThan(f.output(1), Literal(0))) => f
      }.isDefined, "Predicate 'c2 > 0' should be pushed down to the CTE def 'v'.")
      assert(distinctCteRepartitions(0).collectFirst {
        case f: Filter if f.condition.find(_.semanticEquals(f.output(0))).isDefined => f
      }.isDefined, "CTE 'vv' definition contains predicate 'c1 > 0'.")
      assert(distinctCteRepartitions(1).collectFirst {
        case f: Filter if f.condition.find(_.semanticEquals(f.output(0))).isDefined => f
      }.isEmpty, "Predicate 'c1 > 0' should be not pushed down to the CTE def 'v'.")
      // Check runtime repartition reuse.
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "CTE repartition is reused.")
    }
  }

  test("CTE Predicate push-down and column pruning - combined predicate") {
    withTempView("t") {
      Seq((0, 1, 2, 3), (1, 2, 3, 4)).toDF("c1", "c2", "c3", "c4").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, c3, c4, rand() c5 from t
           |),
           |vv as (
           |  select v1.c1, v1.c2, rand() c6 from v v1, v v2
           |  where v1.c1 > 0 and v2.c3 < 5 and v1.c2 = v2.c2
           |)
           |select vv1.c1, vv1.c2, vv2.c1, vv2.c2 from vv vv1, vv vv2
           |where vv1.c2 > 0 and vv2.c2 > 0 and vv1.c1 = vv2.c1
         """.stripMargin)
      checkAnswer(df, Row(1, 2, 1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.length == 6,
        "CTE should not be inlined after optimization.")
      val distinctCteRepartitions = cteRepartitions.map(_.canonicalized).distinct
      // Check column pruning and predicate push-down.
      assert(distinctCteRepartitions.length == 2)
      assert(distinctCteRepartitions(1).collectFirst {
        case p: Project if p.projectList.length == 3 => p
      }.isDefined, "CTE columns should be pruned.")
      assert(
        distinctCteRepartitions(1).collectFirst {
          case f: Filter
              if f.condition.semanticEquals(
                And(
                  GreaterThan(f.output(1), Literal(0)),
                  Or(
                    GreaterThan(f.output(0), Literal(0)),
                    LessThan(f.output(2), Literal(5))))) =>
            f
        }.isDefined,
        "Predicate 'c2 > 0 AND (c1 > 0 OR c3 < 5)' should be pushed down to the CTE def 'v'.")
      // Check runtime repartition reuse.
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "CTE repartition is reused.")
    }
  }

  test("Views with CTEs - 1 temp view") {
    withTempView("t", "t2") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      sql(
        s"""with
           |v as (
           |  select c1 + c2 c3 from t
           |)
           |select sum(c3) s from v
         """.stripMargin).createOrReplaceTempView("t2")
      val df = sql(
        s"""with
           |v as (
           |  select c1 * c2 c3 from t
           |)
           |select sum(c3) from v except select s from t2
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
    }
  }

  test("Views with CTEs - 2 temp views") {
    withTempView("t", "t2", "t3") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      sql(
        s"""with
           |v as (
           |  select c1 + c2 c3 from t
           |)
           |select sum(c3) s from v
         """.stripMargin).createOrReplaceTempView("t2")
      sql(
        s"""with
           |v as (
           |  select c1 * c2 c3 from t
           |)
           |select sum(c3) s from v
         """.stripMargin).createOrReplaceTempView("t3")
      val df = sql("select s from t3 except select s from t2")
      checkAnswer(df, Row(2) :: Nil)
    }
  }

  test("Views with CTEs - temp view + sql view") {
    withTable("t") {
      withTempView ("t2", "t3") {
        Seq((0, 1), (1, 2)).toDF("c1", "c2").write.saveAsTable("t")
        sql(
          s"""with
             |v as (
             |  select c1 + c2 c3 from t
             |)
             |select sum(c3) s from v
           """.stripMargin).createOrReplaceTempView("t2")
        sql(
          s"""create view t3 as
             |with
             |v as (
             |  select c1 * c2 c3 from t
             |)
             |select sum(c3) s from v
           """.stripMargin)
        val df = sql("select s from t3 except select s from t2")
        checkAnswer(df, Row(2) :: Nil)
      }
    }
  }

  test("Union of Dataframes with CTEs") {
    val a = spark.sql("with t as (select 1 as n) select * from t ")
    val b = spark.sql("with t as (select 2 as n) select * from t ")
    val df = a.union(b)
    checkAnswer(df, Row(1) :: Row(2) :: Nil)
  }

  test("CTE definitions out of original order when not inlined") {
    withTempView("issue_current") {
      Seq((1, 2, 10, 100), (2, 3, 20, 200)).toDF("workspace_id", "issue_id", "shard_id", "field_id")
        .createOrReplaceTempView("issue_current")
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.InlineCTE") {
        val df = sql(
          """
            |WITH cte_0 AS (
            |  SELECT workspace_id, issue_id, shard_id, field_id FROM issue_current
            |),
            |cte_1 AS (
            |  WITH filtered_source_table AS (
            |    SELECT * FROM cte_0 WHERE shard_id in ( 10 )
            |  )
            |  SELECT source_table.workspace_id, field_id FROM cte_0 source_table
            |  INNER JOIN (
            |    SELECT workspace_id, issue_id FROM filtered_source_table GROUP BY 1, 2
            |  ) target_table
            |  ON source_table.issue_id = target_table.issue_id
            |  AND source_table.workspace_id = target_table.workspace_id
            |  WHERE source_table.shard_id IN ( 10 )
            |)
            |SELECT * FROM cte_1
        """.stripMargin)
        checkAnswer(df, Row(1, 100) :: Nil)
      }
    }
  }

  test("Make sure CTESubstitution places WithCTE back in the plan correctly.") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")

      // CTE on both sides of join - WithCTE placed over first common parent, i.e., the join.
      val df1 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  with
           |  v1 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v1
           |) v1 join (
           |  with
           |  v2 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v2
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df1, Row(2, 2) :: Nil)
      df1.queryExecution.analyzed match {
        case Aggregate(_, _, WithCTE(_, cteDefs), _) => assert(cteDefs.length == 2)
        case other => fail(s"Expect pattern Aggregate(WithCTE(_)) but got $other")
      }

      // CTE on one side of join - WithCTE placed back where it was.
      val df2 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  select c1, c2, rand() c3 from t
           |) v1 join (
           |  with
           |  v2 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v2
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df2, Row(2, 2) :: Nil)
      df2.queryExecution.analyzed match {
        case Aggregate(_, _, Join(_, SubqueryAlias(_, WithCTE(_, cteDefs)), _, _, _), _) =>
          assert(cteDefs.length == 1)
        case other => fail(s"Expect pattern Aggregate(Join(_, WithCTE(_))) but got $other")
      }

      // CTE on one side of join and both sides of union - WithCTE placed on first common parent.
      val df3 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  select c1, c2, rand() c3 from t
           |) v1 join (
           |  select * from (
           |    with
           |    v1 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v1
           |  )
           |  union all
           |  select * from (
           |    with
           |    v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v2
           |  )
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df3, Row(4, 4) :: Nil)
      df3.queryExecution.analyzed match {
        case Aggregate(_, _, Join(_, SubqueryAlias(_, WithCTE(_: Union, cteDefs)), _, _, _), _) =>
          assert(cteDefs.length == 2)
        case other => fail(
          s"Expect pattern Aggregate(Join(_, (WithCTE(Union(_, _))))) but got $other")
      }

      // CTE on one side of join and one side of union - WithCTE placed back where it was.
      val df4 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  select c1, c2, rand() c3 from t
           |) v1 join (
           |  select * from (
           |    with
           |    v1 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v1
           |  )
           |  union all
           |  select c1, c2, rand() c3 from t
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df4, Row(4, 4) :: Nil)
      df4.queryExecution.analyzed match {
        case Aggregate(_, _, Join(_, SubqueryAlias(_, Union(children, _, _)), _, _, _), _)
          if children.head.find(_.isInstanceOf[WithCTE]).isDefined =>
          assert(
            children.head.collect {
              case w: WithCTE => w
            }.head.cteDefs.length == 1)
        case other => fail(
          s"Expect pattern Aggregate(Join(_, (WithCTE(Union(_, _))))) but got $other")
      }

      // CTE on both sides of join and one side of union - WithCTE placed on first common parent.
      val df5 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  with
           |  v1 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v1
           |) v1 join (
           |  select c1, c2, rand() c3 from t
           |  union all
           |  select * from (
           |    with
           |    v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v2
           |  )
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df5, Row(4, 4) :: Nil)
      df5.queryExecution.analyzed match {
        case Aggregate(_, _, WithCTE(_, cteDefs), _) => assert(cteDefs.length == 2)
        case other => fail(s"Expect pattern Aggregate(WithCTE(_)) but got $other")
      }

      // CTE as root node - WithCTE placed back where it was.
      val df6 = sql(
        s"""
           |with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |)
           |select count(v1.c3), count(v2.c3) from
           |v1 join (
           |  with
           |  v2 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v2
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df6, Row(2, 2) :: Nil)
      df6.queryExecution.analyzed match {
        case WithCTE(_, cteDefs) => assert(cteDefs.length == 2)
        case other => fail(s"Expect pattern WithCTE(_) but got $other")
      }
    }
  }

  test("SPARK-44934: CTE column pruning handles duplicate exprIds in CTE") {
    withTempView("t") {
      Seq((0, 1, 2), (1, 2, 3)).toDF("c1", "c2", "c3").createOrReplaceTempView("t")
      val query =
        """
          |with cte as (
          |  select c1, c1, c2, c3 from t where random() > 0
          |)
          |select cte.c1, cte2.c1, cte.c2, cte2.c3 from
          |  (select c1, c2 from cte) cte
          |    inner join
          |  (select c1, c3 from cte) cte2
          |    on cte.c1 = cte2.c1
          """.stripMargin

      val df = sql(query)
      checkAnswer(df, Row(0, 0, 1, 2) :: Row(1, 1, 2, 3) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 1,
        "With-CTE should contain 1 CTE def after analysis.")
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.length == 2,
        "CTE should not be inlined after optimization.")
      assert(cteRepartitions.head.collectFirst {
        case p: Project if p.projectList.length == 4 => p
      }.isDefined, "CTE columns should not be pruned.")
    }
  }

  test("SPARK-45752: Unreferenced CTE should all be checked by CheckAnalysis0") {
    val e = intercept[AnalysisException](sql(
      s"""
        |with
        |a as (select * from tab_non_exists),
        |b as (select * from a)
        |select 2
        |""".stripMargin))
    checkErrorTableNotFound(e, "`tab_non_exists`", ExpectedContext("tab_non_exists", 26, 39))

    withTable("tab_exists") {
      spark.sql("CREATE TABLE tab_exists(id INT) using parquet")
      val e = intercept[AnalysisException](sql(
        s"""
           |with
           |a as (select * from tab_exists),
           |b as (select * from a),
           |c as (select * from tab_non_exists),
           |d as (select * from c)
           |select 2
           |""".stripMargin))
      checkErrorTableNotFound(e, "`tab_non_exists`", ExpectedContext("tab_non_exists", 83, 96))
    }
  }

  test("SPARK-48307: not-inlined CTE references sibling") {
    val df = sql(
      """
        |WITH
        |v1 AS (SELECT 1 col),
        |v2 AS (SELECT col, rand() FROM v1)
        |SELECT l.col FROM v2 l JOIN v2 r ON l.col = r.col
        |""".stripMargin)
    checkAnswer(df, Row(1))
  }

  test("SPARK-49816: detect self-contained WithCTE nodes") {
    withView("v") {
      sql(
        """
          |WITH
          |t1 AS (SELECT 1 col),
          |t2 AS (SELECT * FROM t1)
          |SELECT * FROM t2
          |""".stripMargin).createTempView("v")
      // r1 is un-referenced, but it should not decrease the ref count of t2 inside view v.
      val df = sql(
        """
          |WITH
          |r1 AS (SELECT * FROM v),
          |r2 AS (SELECT * FROM v)
          |SELECT * FROM r2
          |""".stripMargin)
      checkAnswer(df, Row(1))
    }
  }

  test("SPARK-49816: complicated reference count") {
    // Manually build the logical plan for
    // WITH
    //  r1 AS (SELECT random()),
    //  r2 AS (
    //    WITH
    //      t1 AS (SELECT * FROM r1),
    //      t2 AS (SELECT * FROM r1)
    //    SELECT * FROM t2
    //  )
    // SELECT * FROM r2
    // r1 should be inlined as it's only referenced once: main query -> r2 -> t2 -> r1
    val r1 = CTERelationDef(Project(Seq(Alias(Rand(Literal(0)), "r")()), OneRowRelation()))
    val r1Ref = CTERelationRef(r1.id, r1.resolved, r1.output, r1.isStreaming)
    val t1 = CTERelationDef(Project(r1.output, r1Ref))
    val t2 = CTERelationDef(Project(r1.output, r1Ref))
    val t2Ref = CTERelationRef(t2.id, t2.resolved, t2.output, t2.isStreaming)
    val r2 = CTERelationDef(WithCTE(Project(t2.output, t2Ref), Seq(t1, t2)))
    val r2Ref = CTERelationRef(r2.id, r2.resolved, r2.output, r2.isStreaming)
    val query = WithCTE(Project(r2.output, r2Ref), Seq(r1, r2))
    val inlined = InlineCTE().apply(query)
    assert(!inlined.exists(_.isInstanceOf[WithCTE]))
  }

  test("SPARK-49816: complicated reference count 2") {
    // Manually build the logical plan for
    // WITH
    //  r1 AS (SELECT random()),
    //  r2 AS (
    //    WITH
    //      t1 AS (SELECT * FROM r1),
    //      t2 AS (SELECT * FROM t1)
    //    SELECT * FROM t2
    //  )
    // SELECT * FROM r1
    // This is similar to the previous test case, but t2 reference t1 instead of r1, and the main
    // query references r1. r1 should be inlined as r2 is not referenced at all.
    val r1 = CTERelationDef(Project(Seq(Alias(Rand(Literal(0)), "r")()), OneRowRelation()))
    val r1Ref = CTERelationRef(r1.id, r1.resolved, r1.output, r1.isStreaming)
    val t1 = CTERelationDef(Project(r1.output, r1Ref))
    val t1Ref = CTERelationRef(t1.id, t1.resolved, t1.output, t1.isStreaming)
    val t2 = CTERelationDef(Project(t1.output, t1Ref))
    val t2Ref = CTERelationRef(t2.id, t2.resolved, t2.output, t2.isStreaming)
    val r2 = CTERelationDef(WithCTE(Project(t2.output, t2Ref), Seq(t1, t2)))
    val query = WithCTE(Project(r1.output, r1Ref), Seq(r1, r2))
    val inlined = InlineCTE().apply(query)
    assert(!inlined.exists(_.isInstanceOf[WithCTE]))
  }

  test("SPARK-49816: complicated reference count 3") {
    // Manually build the logical plan for
    // WITH
    //  r1 AS (
    //    WITH
    //      t1 AS (SELECT random()),
    //      t2 AS (SELECT * FROM t1)
    //    SELECT * FROM t2
    //  ),
    //  r2 AS (
    //    WITH
    //      t1 AS (SELECT random()),
    //      t2 AS (SELECT * FROM r1)
    //    SELECT * FROM t2
    //  )
    // SELECT * FROM r1 UNION ALL SELECT * FROM r2
    // The inner WITH in r1 and r2 should become `SELECT random()` and r1/r2 should be inlined.
    val t1 = CTERelationDef(Project(Seq(Alias(Rand(Literal(0)), "r")()), OneRowRelation()))
    val t1Ref = CTERelationRef(t1.id, t1.resolved, t1.output, t1.isStreaming)
    val t2 = CTERelationDef(Project(t1.output, t1Ref))
    val t2Ref = CTERelationRef(t2.id, t2.resolved, t2.output, t2.isStreaming)
    val cte = WithCTE(Project(t2.output, t2Ref), Seq(t1, t2))
    val r1 = CTERelationDef(cte)
    val r1Ref = CTERelationRef(r1.id, r1.resolved, r1.output, r1.isStreaming)
    val r2 = CTERelationDef(cte)
    val r2Ref = CTERelationRef(r2.id, r2.resolved, r2.output, r2.isStreaming)
    val query = WithCTE(Union(r1Ref, r2Ref), Seq(r1, r2))
    val inlined = InlineCTE().apply(query)
    assert(!inlined.exists(_.isInstanceOf[WithCTE]))
  }

  test("SPARK-51109: CTE in subquery expression as grouping column") {
    withTable("t") {
      Seq(1 -> 1).toDF("c1", "c2").write.saveAsTable("t")
      withView("v") {
        sql(
          """
            |CREATE VIEW v AS
            |WITH r AS (SELECT c1 + c2 AS c FROM t)
            |SELECT * FROM r
            |""".stripMargin)
        checkAnswer(
          sql("SELECT (SELECT max(c) FROM v WHERE c > id) FROM range(1) GROUP BY 1"),
          Row(2)
        )
      }
    }
  }

  test("SPARK-51625: command in CTE relations should trigger inline") {
    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("t")),
      cteRelations = Seq(("t", SubqueryAlias("t", ShowTables(CurrentNamespace, pattern = None)),
        None))
    )
    assert(!spark.sessionState.analyzer.execute(plan).exists {
      case _: WithCTE => true
      case _ => false
    })
  }

  test("cte.cache.enabled: expensive multi-ref CTE is materialized") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        val df = sql("""
          WITH cte AS (
            SELECT a.c1, sum(a.c2) as total
            FROM t a JOIN t b ON a.c1 = b.c1
            JOIN t c ON a.c1 = c.c1
            GROUP BY a.c1
          )
          SELECT x.c1, y.total FROM cte x JOIN cte y ON x.c1 = y.c1
        """)
        val plan = df.queryExecution.optimizedPlan
        val hasInMemoryRelation = plan.collect { case _: InMemoryRelation => true }.nonEmpty
        assert(hasInMemoryRelation,
          s"Expensive multi-ref CTE should be materialized with InMemoryRelation," +
            s" plan:\n${plan.treeString}")
        // Verify correctness
        withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
          checkAnswer(df, sql("""
            WITH cte AS (
              SELECT a.c1, sum(a.c2) as total
              FROM t a JOIN t b ON a.c1 = b.c1
              JOIN t c ON a.c1 = c.c1
              GROUP BY a.c1
            )
            SELECT x.c1, y.total FROM cte x JOIN cte y ON x.c1 = y.c1
          """))
        }
        spark.sharedState.cacheManager.clearCache()
      }
    }
  }

  test("cte.cache.enabled: cheap CTE still inlined") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "true") {
        val df = sql("""
          WITH cte AS (SELECT c1, c2 FROM t WHERE c1 > 1)
          SELECT x.c1, y.c2 FROM cte x JOIN cte y ON x.c1 = y.c1
        """)
        val plan = df.queryExecution.optimizedPlan
        val hasRepartition = plan.treeString.contains("RepartitionByExpression")
        assert(!hasRepartition, "Cheap CTE should still be inlined (no repartition)")
      }
    }
  }

  test("cte.cache.enabled: single-ref expensive CTE still inlined") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        val df = sql("""
          WITH cte AS (
            SELECT a.c1, sum(a.c2) as total
            FROM t a JOIN t b ON a.c1 = b.c1
            JOIN t c ON a.c1 = c.c1
            GROUP BY a.c1
          )
          SELECT c1, total FROM cte WHERE total > 10
        """)
        val plan = df.queryExecution.optimizedPlan
        val hasRepartition = plan.treeString.contains("RepartitionByExpression")
        assert(!hasRepartition, "Single-ref CTE should always be inlined")
      }
    }
  }

  test("cte.cache.enabled: disabled by default") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        val df = sql("""
          WITH cte AS (
            SELECT a.c1, sum(a.c2) as total
            FROM t a JOIN t b ON a.c1 = b.c1
            JOIN t c ON a.c1 = c.c1
            GROUP BY a.c1
          )
          SELECT x.c1, y.total FROM cte x JOIN cte y ON x.c1 = y.c1
        """)
        val plan = df.queryExecution.optimizedPlan
        val hasRepartition = plan.treeString.contains("RepartitionByExpression")
        assert(!hasRepartition, "Default config should inline (old behavior)")
      }
    }
  }

  test("cte.cache.enabled: UNION CTE still inlined") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        val df = sql("""
          WITH cte AS (
            SELECT c1, c2 FROM t WHERE c1 > 1
            UNION ALL
            SELECT c1, c2 FROM t WHERE c1 < 3
          )
          SELECT x.c1, y.c2 FROM cte x JOIN cte y ON x.c1 = y.c1
        """)
        val plan = df.queryExecution.optimizedPlan
        val hasRepartition = plan.treeString.contains("RepartitionByExpression")
        assert(!hasRepartition, "UNION CTE should still be inlined")
      }
    }
  }

  test("cte.cache.enabled: correctness with complex CTE") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("c1", "c2").createOrReplaceTempView("t")
      val query = """
        WITH cte AS (
          SELECT a.c1 as k, count(*) as cnt, sum(b.c2) as total
          FROM t a JOIN t b ON a.c1 = b.c1
          JOIN t c ON a.c1 = c.c1
          GROUP BY a.c1
        )
        SELECT x.k, x.cnt, y.total
        FROM cte x, cte y
        WHERE x.k = y.k AND x.cnt > 0
        ORDER BY x.k
      """
      val expected = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql(query).collect()
      }
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        checkAnswer(sql(query), expected)
        spark.sharedState.cacheManager.clearCache()
      }
    }
  }

  test("cte.cache.enabled: non-deterministic CTE uses repartition (not cache)") {
    withTempView("t") {
      Seq((1, 10), (2, 20)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "true") {
        val df = sql("""
          WITH cte AS (SELECT c1, rand() as r FROM t)
          SELECT x.c1, y.r FROM cte x JOIN cte y ON x.c1 = y.c1
        """)
        val plan = df.queryExecution.optimizedPlan
        // Non-deterministic CTE should NOT use InMemoryRelation
        val hasInMemory = plan.collect {
          case _: InMemoryRelation => true
        }.nonEmpty
        assert(!hasInMemory,
          "Non-deterministic CTE should use repartition, not InMemoryRelation cache")
        // Should have repartition instead
        assert(plan.treeString.contains("Repartition"),
          "Non-deterministic CTE should use RepartitionByExpression")
      }
    }
  }

  test("cte.cache.enabled: cache reuse across queries") {
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("c1", "c2").createOrReplaceTempView("t")
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        // First query caches the CTE (multi-ref to avoid inlining)
        val q = """
          WITH cte AS (
            SELECT a.c1, sum(a.c2) as total
            FROM t a JOIN t b ON a.c1 = b.c1
            GROUP BY a.c1
          )
          SELECT x.c1, y.total FROM cte x JOIN cte y ON x.c1 = y.c1
        """
        sql(q).collect()

        // Check cache exists
        assert(!spark.sharedState.cacheManager.isEmpty,
          "CTE should be cached after first query")

        // Second identical query should reuse cache
        sql(q).collect()

        spark.sharedState.cacheManager.clearCache()
      }
    }
  }

  test("cte.cache.enabled: correctness with different column pruning across queries") {
    withTempView("t") {
      Seq((1, 10, 100), (1, 20, 200), (2, 30, 300), (2, 40, 400))
        .toDF("month", "value", "extra")
        .createOrReplaceTempView("t")

      val expected1 = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql("""
          WITH inv AS (
            SELECT a.month, a.value, a.extra
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value, a.extra
          )
          SELECT x.month, x.value, y.month, y.value
          FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 2 AND x.value = y.value
        """).collect()
      }
      val expected2 = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql("""
          WITH inv AS (
            SELECT a.month, a.value, a.extra
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value, a.extra
          )
          SELECT x.month, x.extra, y.month, y.extra
          FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 2 AND x.value = y.value
        """).collect()
      }

      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        checkAnswer(sql("""
          WITH inv AS (
            SELECT a.month, a.value, a.extra
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value, a.extra
          )
          SELECT x.month, x.value, y.month, y.value
          FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 2 AND x.value = y.value
        """), expected1)

        checkAnswer(sql("""
          WITH inv AS (
            SELECT a.month, a.value, a.extra
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value, a.extra
          )
          SELECT x.month, x.extra, y.month, y.extra
          FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 2 AND x.value = y.value
        """), expected2)

        spark.sharedState.cacheManager.clearCache()
      }
    }
  }

  test("cte.cache.enabled: different predicates produce correct results") {
    // Correctness test: two queries with the same CTE structure but different pushed
    // predicates must NOT reuse each other's cache.
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("month", "value").createOrReplaceTempView("t")

      // Compute expected results with cache OFF for both queries
      val expected1 = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql("""
          WITH inv AS (
            SELECT a.month, a.value
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value
          )
          SELECT x.month, y.month FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 2
        """).collect()
      }
      val expected2 = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql("""
          WITH inv AS (
            SELECT a.month, a.value
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value
          )
          SELECT x.month, y.month FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 3
        """).collect()
      }

      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {

        // Query 1: preds (month=1 OR month=2)
        checkAnswer(sql("""
          WITH inv AS (
            SELECT a.month, a.value
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value
          )
          SELECT x.month, y.month FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 2
        """), expected1)

        // Query 2: preds (month=1 OR month=3) - must NOT reuse q1's cache
        checkAnswer(sql("""
          WITH inv AS (
            SELECT a.month, a.value
            FROM t a JOIN t b ON a.month = b.month
            GROUP BY a.month, a.value
          )
          SELECT x.month, y.month FROM inv x, inv y
          WHERE x.month = 1 AND y.month = 3
        """), expected2)

        spark.sharedState.cacheManager.clearCache()
      }
    }
  }

  test("cte.cache.enabled: asymmetric filters - one ref filtered, one bare") {
    // When one CTE reference has a filter and another doesn't, the pushdown rule
    // produces a TRUE predicate (no filtering). Verify correctness.
    withTempView("t") {
      Seq((1, 10), (2, 20), (3, 30)).toDF("c1", "c2").createOrReplaceTempView("t")
      val query = """
        WITH cte AS (
          SELECT a.c1, sum(b.c2) as total
          FROM t a JOIN t b ON a.c1 = b.c1
          GROUP BY a.c1
        )
        SELECT x.c1, x.total, y.c1, y.total
        FROM cte x, cte y
        WHERE x.c1 = 1 AND x.c1 = y.c1
      """
      val expected = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql(query).collect()
      }
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        checkAnswer(sql(query), expected)
        spark.sharedState.cacheManager.clearCache()
      }
    }
  }

  test("cte.cache.enabled: CTE referenced in scalar subquery (HAVING clause)") {
    // Simulates TPC-DS q24a pattern: CTE referenced once in main query with a filter
    // and once in a HAVING scalar subquery without a filter. The subquery reference
    // has no predicate above it, so the combined predicate becomes TRUE.
    withTempView("t") {
      Seq(("red", 10), ("red", 20), ("blue", 30), ("blue", 5))
        .toDF("color", "amount").createOrReplaceTempView("t")
      val query = """
        WITH ssales AS (
          SELECT a.color, sum(b.amount) as total
          FROM t a JOIN t b ON a.color = b.color
          GROUP BY a.color
        )
        SELECT color, total
        FROM ssales
        WHERE color = 'red'
          AND total > (SELECT avg(total) * 0.5 FROM ssales)
        ORDER BY color
      """
      val expected = withSQLConf(SQLConf.CTE_CACHE_ENABLED.key -> "false") {
        sql(query).collect()
      }
      withSQLConf(
        SQLConf.CTE_CACHE_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
        checkAnswer(sql(query), expected)
        spark.sharedState.cacheManager.clearCache()
      }
    }
  }
}

class CTEInlineSuiteAEOff extends CTEInlineSuiteBase with DisableAdaptiveExecutionSuite

class CTEInlineSuiteAEOn extends CTEInlineSuiteBase with EnableAdaptiveExecutionSuite {
  import testImplicits._

  test("SPARK-40105: Improve repartition in ReplaceCTERefWithRepartition") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select /*+ rebalance(c1) */ c1, c2, rand() from t
           |)
           |select * from v except select * from v
         """.stripMargin)
      checkAnswer(df, Nil)

      assert(!df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]))
      assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RebalancePartitions]))
    }
  }
}
