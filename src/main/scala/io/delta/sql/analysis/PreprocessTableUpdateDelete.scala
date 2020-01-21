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
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Delete, DeleteWithJoinTable, LogicalPlan, MergeAction, UpdateTable, UpdateWithJoinTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.commands.{DeleteCommand, DeleteWithJoinCommand, UpdateCommand, UpdateWithJoinCommand}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaFullTable, UpdateExpressionsSupport}
import org.apache.spark.sql.internal.SQLConf

class PreprocessTableUpdateDelete(
    spark: SparkSession) extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case UpdateTable(table, updateColumns, updateExpressions, condition) =>
      condition.foreach(checkCondition(_, "update"))
      val index = EliminateSubqueryAliases(table) match {
        case DeltaFullTable(tahoeFileIndex) => tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
      }
      val resolveNameParts = updateColumns.map { col =>
        UpdateTable.getNameParts(col, "", table)
      }
      val alignedUpdateExprs = generateUpdateExpressions(
        table.output, resolveNameParts, updateExpressions, conf.resolver)
      UpdateCommand(index, table, alignedUpdateExprs, condition)

    case UpdateWithJoinTable(target, source, updateColumns, updateExpressions, condition,
         updateClause) =>
      condition.foreach(checkCondition(_, "update"))
      val index = EliminateSubqueryAliases(target) match {
        case DeltaFullTable(tahoeFileIndex) => tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
      }

      val resolveNameParts = updateColumns.map { col =>
        UpdateTable.getNameParts(col, "", target)
      }
      val alignedUpdateExprs = generateUpdateExpressions(
        target.output, resolveNameParts, updateExpressions, conf.resolver)
      val alignedActions: Seq[MergeAction] = alignedUpdateExprs.zip(target.output).map {
        case (expr, attrib) => MergeAction(Seq(attrib.name), expr)
      }
      val update = updateClause.copy(updateClause.condition, alignedActions)

      UpdateWithJoinCommand(source, target, index, condition, update)

    case Delete(table, condition) =>
      val index = EliminateSubqueryAliases(table) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }
      val command = DeleteCommand(index, table, condition)
      spark.sessionState.analyzer.checkAnalysis(command)
      command

    case DeleteWithJoinTable(target, source, condition, delete) =>
      checkCondition(condition, "delete")
      val index = EliminateSubqueryAliases(target) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }
      val command = DeleteWithJoinCommand(source, target, index, condition, delete)
      spark.sessionState.analyzer.checkAnalysis(command)
      command
  }

  private def checkCondition(cond: Expression, conditionName: String): Unit = {
    if (!cond.deterministic) {
      throw DeltaErrors.nonDeterministicNotSupportedException(
        s"$conditionName condition of UPDATE operation", cond)
    }
    if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
      throw DeltaErrors.aggsNotSupportedException(
        s"$conditionName condition of UPDATE operation", cond)
    }
    if (SubqueryExpression.hasSubquery(cond)) {
      throw DeltaErrors.subqueryNotSupportedException(
        s"$conditionName condition of UPDATE operation", cond)
    }
  }

  override def conf: SQLConf = spark.sessionState.conf
}
