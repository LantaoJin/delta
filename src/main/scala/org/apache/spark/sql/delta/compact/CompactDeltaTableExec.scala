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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.delta.commands.ConvertToDeltaCommand
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, InsertIntoDataSourceDirCommand}
import org.apache.spark.sql.execution.{BaseCompactTableExec, SparkPlan}

/**
 * Command for compacting files in a table or partitions.
 */
case class CompactDeltaTableExec(
    table: CatalogTable,
    partitions: Seq[CatalogTablePartition],
    children: Seq[SparkPlan]) extends BaseCompactTableExec(table, partitions, children) {

  override def postHoc: Unit = {
    ConvertToDeltaCommand(table.identifier, None, None).run(SparkSession.getActiveSession.get)
  }

  override def getOutputPath(plan: SparkPlan): String = {
    val storage = plan match {
      case ExecutedCommandExec(InsertIntoDataSourceDirCommand(s, _, _, _)) => s
      case _ => throw new IllegalStateException()
    }
    storage.locationUri.get.getPath
  }

  override def nodeName: String = "CompactDeltaTableExec"
}
