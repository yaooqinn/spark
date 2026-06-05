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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, UNION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Collapses per-branch inner Aggregates inside an Aggregate-over-Union pattern
 * into a single outer Aggregate.
 *
 * Match shape:
 * {{{
 *   Aggregate(g_out, outerAggs,
 *     Union(
 *       Aggregate(g_in, innerAggs, branch_1),
 *       Aggregate(g_in, innerAggs, branch_2),
 *       ...))
 * }}}
 *
 * After rewrite each inner Aggregate is dropped; its underlying expression
 * is exposed via a Project, and the outer Aggregate sees raw per-row values:
 * {{{
 *   Aggregate(g_out, outerAggs,
 *     Union(
 *       Project(g_in ++ summed_args, branch_1),
 *       Project(g_in ++ summed_args, branch_2),
 *       ...))
 * }}}
 *
 * Algebraic validity: `SUM` and `COUNT` distribute over a single outer SUM
 * because SUM(SUM(x)) ≡ SUM(x) and COUNT(x) ≡ SUM(IF(x IS NOT NULL, 1L, 0L)).
 * Other aggregate kinds (AVG, DISTINCT-bearing, etc.) are not algebraically
 * collapsible and are rejected by [[classifySlot]].
 *
 * Each outer Aggregate touched by this rule is tagged with
 * [[PullUpJoinFromUnion.COLLAPSED_TAG]] so subsequent FixedPoint passes
 * do not re-enter the same node.
 *
 * Literal aliases inside inner aggregate outputs (e.g.
 * `'web' AS channel`) are intentionally rejected via [[SlotOther]]:
 * collapsing them changes row multiplicity when the outer Aggregate
 * does not group by the literal.
 */
object PullUpJoinFromUnion {
  /** Marker so a collapsed Aggregate is skipped on a second pass — idempotency safety. */
  val COLLAPSED_TAG: TreeNodeTag[Unit] = TreeNodeTag[Unit]("pullUpJoinFromUnion.collapsed")
}

case class PullUpJoinFromUnion(override val conf: SQLConf) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.PULL_UP_JOIN_FROM_UNION_ENABLED)) return plan

    plan.transformUpWithPruning(
      _.containsAllPatterns(AGGREGATE, UNION), ruleId) {
      case agg @ Aggregate(_, _, u: Union, _)
          if agg.getTagValue(PullUpJoinFromUnion.COLLAPSED_TAG).isEmpty =>
        tryRewrite(agg, u).getOrElse(agg)
    }
  }

  private def tryRewrite(outerAgg: Aggregate, u: Union): Option[LogicalPlan] = {
    if (u.children.size < 2) return None

    // Every branch must be a plain Aggregate of the same arity.
    val innerAggs = u.children.collect { case a: Aggregate => a }
    if (innerAggs.size != u.children.size) return None
    val outArity = innerAggs.head.aggregateExpressions.size
    if (innerAggs.exists(_.aggregateExpressions.size != outArity)) return None

    // Classify each output slot across branches; abort on any SlotOther.
    val slotKinds: Seq[SlotKind] = (0 until outArity).map { idx =>
      val perBranch = innerAggs.map { a =>
        classifySlot(a.aggregateExpressions(idx), a.groupingExpressions)
      }
      if (perBranch.contains(SlotOther)) SlotOther
      else if (perBranch.forall(_ == SlotGroupKey)) SlotGroupKey
      else if (perBranch.forall(_ == SlotSumAll)) SlotSumAll
      else if (perBranch.forall(_ == SlotCountAll)) SlotCountAll
      else SlotOther
    }
    if (slotKinds.contains(SlotOther)) return None

    // Build Project for each branch: group-keys pass through, SUM/COUNT slots
    // expose their argument so the outer SUM sees raw values.
    val unionOut = innerAggs.head.output
    val rewrittenBranches = innerAggs.flatMap { a =>
      val maybeExprs = a.aggregateExpressions.zipWithIndex.foldLeft(
        Option(Vector.empty[NamedExpression])) {
        case (None, _) => None
        case (Some(acc), (origNE, i)) =>
          val targetAttr = unionOut(i)
          slotKinds(i) match {
            case SlotGroupKey =>
              Some(acc :+ origNE)
            case SlotSumAll | SlotCountAll =>
              extractArg(origNE).map { inner =>
                acc :+ Alias(inner, targetAttr.name)(
                  exprId = targetAttr.exprId,
                  qualifier = targetAttr.qualifier)
              }
            case SlotOther =>
              None
          }
      }
      maybeExprs.map(exprs => Project(exprs, a.child))
    }
    // Any branch that failed to rewrite (extractArg returned None) bails the whole rewrite.
    if (rewrittenBranches.size != innerAggs.size) return None

    val newUnion = Union(rewrittenBranches)
    val newAgg = Aggregate(
      outerAgg.groupingExpressions,
      outerAgg.aggregateExpressions,
      newUnion,
      outerAgg.hint)
    newAgg.setTagValue(PullUpJoinFromUnion.COLLAPSED_TAG, ())
    Some(newAgg)
  }

  /**
   * Extracts the underlying expression that the outer Aggregate must SUM over.
   * SUM(x) → x; COUNT(x) → IF(x IS NOT NULL, 1L, 0L); COUNT(*) → 1L.
   * Returns None for any other shape so the caller aborts the rewrite.
   */
  private def extractArg(ne: NamedExpression): Option[Expression] = ne match {
    case Alias(AggregateExpression(s: Sum, _, _, _, _), _) =>
      Some(s.child)
    case Alias(AggregateExpression(Count(cs), _, _, _, _), _) =>
      cs match {
        case Seq(Literal(1, _)) => Some(Literal(1L))
        case Seq(one) => Some(If(IsNotNull(one), Literal(1L), Literal(0L)))
        case multi =>
          val cond = multi.map(IsNotNull).reduce(And)
          Some(If(cond, Literal(1L), Literal(0L)))
      }
    case _ => None
  }

  sealed trait SlotKind
  case object SlotGroupKey extends SlotKind
  case object SlotSumAll extends SlotKind
  case object SlotCountAll extends SlotKind
  case object SlotOther extends SlotKind

  private def classifySlot(ne: NamedExpression, groupKeys: Seq[Expression]): SlotKind = ne match {
    case Alias(AggregateExpression(s: Sum, Complete, false, None, _), _)
        if s.child.deterministic && !SubqueryExpression.hasSubquery(s.child) => SlotSumAll
    case Alias(AggregateExpression(_: Count, Complete, false, None, _), _) => SlotCountAll
    case Alias(child, _) if groupKeys.exists(g => child.semanticEquals(g)) => SlotGroupKey
    case _: AttributeReference if groupKeys.exists(g => ne.semanticEquals(g)) => SlotGroupKey
    case _ => SlotOther
  }
}
