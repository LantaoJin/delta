/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sql.analysis

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue, caseInsensitiveResolution, withPosition}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, CurrentDate, CurrentTimestamp, Expression, ExtractValue}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.delta.DeltaErrors

class DeltaSqlResolution(spark: SparkSession) extends Rule[LogicalPlan] {

  private def resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateTableStatement(table, assignments, condition, None)
        if !u.resolved && table.resolved =>
      checkTargetTable(table)
      val resolvedAssignments = resolveAssignments(assignments, u)
      val columns = resolvedAssignments.map(_.key.asInstanceOf[Attribute])
      val values = resolvedAssignments.map(_.value)
      val resolvedUpdateCondition = condition.map(resolveExpression(_, u))
      UpdateTable(table, columns, values, resolvedUpdateCondition)

    case u @ UpdateTableStatement(target, assignments, condition, Some(source))
        if !u.resolved && target.resolved && source.resolved &&
          target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      checkTargetTable(target)
      val resolvedAssignments = resolveAssignments(assignments, u)
      val columns = resolvedAssignments.map(_.key.asInstanceOf[Attribute])
      val values = resolvedAssignments.map(_.value)
      val resolvedUpdateCondition = condition.map(resolveExpression(_, u))
      // todo (lajin) we can use assignments.forall(_.foldable) after change to
      // 'override def foldable: Boolean = value.foldable' in class Assignment in Spark
      if (condition.isEmpty && values.forall(_.foldable)) {
        // update with join with no condition equals condition is true
        // if SET expressions are all foldable, skip join and fallback to simple update
        UpdateTable(target, columns, values, resolvedUpdateCondition)
      } else {
        val updateActions = UpdateTable.toActionFromAssignments(resolvedAssignments)
        val actions = DeltaMergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
        UpdateWithJoinTable(target, source, columns, values, resolvedUpdateCondition, actions)
      }

    case d @ DeleteFromStatement(table, condition, None)
        if !d.resolved && table.resolved =>
      checkTargetTable(table)
      Delete(table, condition)

    case d @ DeleteFromStatement(target, condition, Some(source))
        if !d.resolved && target.resolved && source.resolved &&
          target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
      checkTargetTable(target)
      val resolvedDeleteCondition = condition.map(resolveExpression(_, d))
      val actions = DeltaMergeIntoDeleteClause(resolvedDeleteCondition)
      DeleteWithJoinTable(target, source, resolvedDeleteCondition, actions)

//    case m @ MergeIntoTableStatement(targetTable, sourceTable,
//        mergeCondition, matchedActions, notMatchedActions)
//        if !m.resolved && targetTable.resolved && sourceTable.resolved &&
//          target.outputSet.intersect(source.outputSet).isEmpty => // no conflicting attributes
//      val resolvedMergeCondition = resolveExpression(mergeCondition, m)
//      val newMatchedActions = matchedActions.collect {
//        case DeleteAction(deleteCondition) =>
//          val resolvedDeleteCondition = deleteCondition.map(resolveExpression(_, m))
//          MergeIntoDeleteClause(resolvedDeleteCondition)
//        case UpdateAction(updateCondition, assignments) =>
//          val resolvedUpdateCondition = updateCondition.map(resolveExpression(_, m))
//          val resolvedAssignments = resolveAssignments(assignments, m)
//          val updateActions = MergeIntoClause.toActionFromAssignments(resolvedAssignments)
//          MergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
//      }
//      val newNotMatchedActions = notMatchedActions.collectFirst {
//        case InsertAction(insertCondition, assignments) =>
//          val resolvedInsertCondition = insertCondition.map(resolveExpression(_, m))
//          val resolvedAssignments = resolveAssignments(assignments, m)
//          val insertActions = MergeIntoClause.toActionFromAssignments(resolvedAssignments)
//          MergeIntoInsertClause(resolvedInsertCondition, insertActions)
//      }
//      MergeInto(targetTable, sourceTable,
//        resolvedMergeCondition, newMatchedActions, newNotMatchedActions)
  }

  private def resolveAssignments(
      assignments: Seq[Assignment],
      update: LogicalPlan): Seq[Assignment] = {
    assignments.map { assign =>
      val resolvedKey = assign.key match {
        case c if !c.resolved => resolveExpression(c, update)
        case o => o
      }
      val resolvedValue = assign.value match {
        case c if !c.resolved => resolveExpression(Cast(c, resolvedKey.dataType), update)
        case o => o
      }
      Assignment(resolvedKey, resolvedValue)
    }
  }

  private def resolveExpression(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false): Expression = {
    if (expr.resolved) return expr
    // Resolve expression in one round.
    // If throws == false or the desired attribute doesn't exist
    // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
    // Else, throw exception.
    try {
      expr transformDown {
        case GetColumnByOrdinal(ordinal, _) => plan.output(ordinal)
        case u @ UnresolvedAttribute(nameParts) =>
          withPosition(u) {
            plan.resolveChildren(nameParts, resolver)
              .orElse(resolveLiteralFunction(nameParts, u, plan))
              .getOrElse(u)
          }
        case UnresolvedExtractValue(child, fieldName) if child.resolved =>
          ExtractValue(child, fieldName, resolver)
      }
    } catch {
      case a: AnalysisException if !throws => expr
    }
  }

  /**
   * Literal functions do not require the user to specify braces when calling them
   * When an attributes is not resolvable, we try to resolve it as a literal function.
   */
  private def resolveLiteralFunction(
      nameParts: Seq[String],
      attribute: UnresolvedAttribute,
      plan: LogicalPlan): Option[Expression] = {
    if (nameParts.length != 1) return None
    val isNamedExpression = plan match {
      case Aggregate(_, aggregateExpressions, _) => aggregateExpressions.contains(attribute)
      case Project(projectList, _) => projectList.contains(attribute)
      case Window(windowExpressions, _, _, _) => windowExpressions.contains(attribute)
      case _ => false
    }
    val wrapper: Expression => Expression =
      if (isNamedExpression) f => Alias(f, toPrettySQL(f))() else identity
    // support CURRENT_DATE and CURRENT_TIMESTAMP
    val literalFunctions = Seq(CurrentDate(), CurrentTimestamp())
    val name = nameParts.head
    val func = literalFunctions.find(e => caseInsensitiveResolution(e.prettyName, name))
    func.map(wrapper)
  }

  private def checkTargetTable(targetTable: LogicalPlan): Unit = {
    targetTable.collect {
      case View(t, _, _) =>
        throw DeltaErrors.cannotUpdateAViewException(t.identifier)
    }
  }
}
