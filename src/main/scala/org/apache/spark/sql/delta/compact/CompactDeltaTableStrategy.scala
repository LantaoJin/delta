/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.compact

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CompactTableCommand, DDLUtils}
import org.apache.spark.sql.execution.{PlanLater, SparkPlan}

object CompactDeltaTableStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CompactTableCommand(parquetTableWithDeltaProvider, partitions, plans)
      if DDLUtils.isDeltaTable(parquetTableWithDeltaProvider) =>
      // Since we set delta in CompactDeltaTableAnalysis, we should set it back
      CompactDeltaTableExec(
        parquetTableWithDeltaProvider.copy(provider = Some("parquet")),
        partitions, plans.map(PlanLater)) :: Nil
    case _ => Nil
  }
}
