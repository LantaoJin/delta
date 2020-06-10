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

package io.delta.tables.execution

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.services.DeltaTableMetadata
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Encoders, Row, SparkSession}

case class ShowDeltasCommand() extends RunnableCommand with DeltaCommand {
  import org.apache.spark.sql.delta.services.DeltaTableMetadata._

  override val output: Seq[Attribute] = Encoders.product[DeltaTableMetadata].schema
    .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  override def run(spark: SparkSession): Seq[Row] = {
    getRowsFromMetadataTable(spark)
  }
}
