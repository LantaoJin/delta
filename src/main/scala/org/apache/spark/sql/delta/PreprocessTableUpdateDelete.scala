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

package org.apache.spark.sql.delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.commands.{DeleteCommand, DeleteWithJoinCommand, UpdateCommand, UpdateWithJoinCommand}
import org.apache.spark.sql.internal.SQLConf

class PreprocessTableUpdateDelete(
    spark: SparkSession) extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ DeltaUpdateTable(table, updateColumns, updateExpressions, condition) if u.resolved =>
      condition.foreach(checkCondition(_, "update"))
      val index = EliminateSubqueryAliases(table) match {
        case DeltaFullTable(tahoeFileIndex) => tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
      }
      val resolveNameParts = updateColumns.map { col =>
        DeltaUpdateTable.getTargetColNameParts(col, "")
      }
      val alignedUpdateExprs = generateUpdateExpressions(
        table.output, resolveNameParts, updateExpressions, conf.resolver)
      UpdateCommand(index, table, alignedUpdateExprs, condition)

    case u @ UpdateWithJoinTable(target, source, updateColumns, updateExpressions, condition,
         updateClause) if u.resolved =>
      condition.foreach(checkCondition(_, "update"))
      val index = EliminateSubqueryAliases(target) match {
        case DeltaFullTable(tahoeFileIndex) => tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
      }

      val resolveNameParts = updateColumns.map { col =>
        DeltaUpdateTable.getTargetColNameParts(col, "")
      }
      val alignedUpdateExprs = generateUpdateExpressions(
        target.output, resolveNameParts, updateExpressions, conf.resolver)
      val alignedActions = alignedUpdateExprs.zip(target.output).map {
        case (expr, attrib) => DeltaMergeAction(Seq(attrib.name), expr)
      }
      val update = updateClause.copy(updateClause.condition, alignedActions)

      UpdateWithJoinCommand(source, target, index, condition, update)

    case d @ DeltaDelete(table, condition) if d.resolved =>
      val index = EliminateSubqueryAliases(table) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }
      DeleteCommand(index, table, condition)

    case d @ DeleteWithJoinTable(target, source, condition, delete) if d.resolved =>
      condition.foreach(checkCondition(_, "delete"))
      val index = EliminateSubqueryAliases(target) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }
      DeleteWithJoinCommand(source, target, index, condition, delete)
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
