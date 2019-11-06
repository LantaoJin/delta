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
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.logical.{Delete, LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.commands.{DeleteCommand, UpdateCommand}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaFullTable, UpdateExpressionsSupport}
import org.apache.spark.sql.internal.SQLConf

class PreprocessTableUpdateDelete(
    spark: SparkSession) extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case update: UpdateTable =>
      val index = EliminateSubqueryAliases(update.child) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
      }

      val targetColNameParts =
        update.updateColumns.map{col => new UnresolvedAttribute(col.name.split("\\.")).nameParts}
      val alignedUpdateExprs = generateUpdateExpressions(
        update.child.output, targetColNameParts, update.updateExpressions, conf.resolver)
      val command = UpdateCommand(index, update.child, alignedUpdateExprs, update.condition)
      spark.sessionState.analyzer.checkAnalysis(command)
      command

    case delete: Delete =>
      val index = EliminateSubqueryAliases(delete.child) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }
      val command = DeleteCommand(index, delete.child, delete.condition)
      spark.sessionState.analyzer.checkAnalysis(command)
      command
  }

  override def conf: SQLConf = spark.sessionState.conf
}
