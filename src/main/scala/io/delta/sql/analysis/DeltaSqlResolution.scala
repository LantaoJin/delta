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
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{Delete, LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{DeleteFromStatement, UpdateTableStatement}

class DeltaSqlResolution(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UpdateTableStatement(tableIdentifier, c, values, condition) =>
      val table = UnresolvedRelation(tableIdentifier)
      val columns = c.map(UnresolvedAttribute(_))
      UpdateTable(table, columns, values, condition)

    case DeleteFromStatement(tableIdentifier, condition) =>
      val table = UnresolvedRelation(tableIdentifier)
      Delete(table, condition)
  }
}
