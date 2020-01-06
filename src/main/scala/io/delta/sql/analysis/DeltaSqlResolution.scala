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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, caseInsensitiveResolution}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentDate, CurrentTimestamp, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.datasources._

class DeltaSqlResolution(spark: SparkSession) extends Rule[LogicalPlan] {

  private def resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateTableStatement(target, assignments, condition, source)
        if !u.resolved && target.resolved &&
          (if (source.isDefined) source.exists(_.resolved) else true) =>
      val resolvedAssignments = resolveAssignments(assignments, u)
      val columns = resolvedAssignments.map(_.key.asInstanceOf[Attribute])
      val values = resolvedAssignments.map(_.value)
      val resolvedUpdateCondition = condition.map(resolveExpressionTopDown(_, u))
      source match {
        case Some(s) =>
          val updateActions = UpdateTable.toActionFromAssignments(resolvedAssignments)
          val actions = MergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
          UpdateWithJoinTable(target, s, columns, values, condition.get, actions)
        case None =>
          UpdateTable(target, columns, values, resolvedUpdateCondition)
      }

    case d @ DeleteFromStatement(table, condition)
      if !d.resolved && table.resolved =>
      Delete(table, condition)

//    case m @ MergeIntoTableStatement(targetTable, sourceTable,
//        mergeCondition, matchedActions, notMatchedActions)
//      if !m.resolved && targetTable.resolved && sourceTable.resolved =>
//      val resolvedMergeCondition = resolveExpressionTopDown(mergeCondition, m)
//      val newMatchedActions = matchedActions.collect {
//        case DeleteAction(deleteCondition) =>
//          val resolvedDeleteCondition = deleteCondition.map(resolveExpressionTopDown(_, m))
//          MergeIntoDeleteClause(resolvedDeleteCondition)
//        case UpdateAction(updateCondition, assignments) =>
//          val resolvedUpdateCondition = updateCondition.map(resolveExpressionTopDown(_, m))
//          val resolvedAssignments = resolveAssignments(assignments, m)
//          val updateActions = MergeIntoClause.toActionFromAssignments(resolvedAssignments)
//          MergeIntoUpdateClause(resolvedUpdateCondition, updateActions)
//      }
//      val newNotMatchedActions = notMatchedActions.collectFirst {
//        case InsertAction(insertCondition, assignments) =>
//          val resolvedInsertCondition = insertCondition.map(resolveExpressionTopDown(_, m))
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
        case c if !c.resolved => resolveExpressionTopDown(c, update)
        case o => o
      }
      val resolvedValue = assign.value match {
        case c if !c.resolved => resolveExpressionTopDown(c, update)
        case o => o
      }
      Assignment(resolvedKey, resolvedValue)
    }
  }

  private def resolveAssignments(
      assignments: Seq[Assignment],
      mergeInto: MergeIntoTableStatement): Seq[Assignment] = {
    if (assignments.isEmpty) {
      val expandedColumns = mergeInto.targetTable.output
      val expandedValues = mergeInto.sourceTable.output
      expandedColumns.zip(expandedValues).map(kv => Assignment(kv._1, kv._2))
    } else {
      assignments.map { assign =>
        val resolvedKey = assign.key match {
          case c if !c.resolved => resolveExpressionTopDown(c, mergeInto.targetTable)
          case o => o
        }
        val resolvedValue = assign.value match {
          // The update values may contain target and/or source references.
          case c if !c.resolved => resolveExpressionTopDown(c, mergeInto)
          case o => o
        }
        Assignment(resolvedKey, resolvedValue)
      }
    }
  }

  private def resolveExpressionTopDown(e: Expression, q: LogicalPlan): Expression = {
    if (e.resolved) return e
    e match {
      case u @ UnresolvedAttribute(nameParts) =>
        // Leave unchanged if resolution fails. Hopefully will be resolved next round.
        val result =
          analysis.withPosition(u) {
            q.resolveChildren(nameParts, resolver)
              .orElse(resolveLiteralFunction(nameParts, u, q))
              .getOrElse(u)
          }
        logDebug(s"Resolving $u to $result")
        result
      case _ => e.mapChildren(resolveExpressionTopDown(_, q))
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
}
