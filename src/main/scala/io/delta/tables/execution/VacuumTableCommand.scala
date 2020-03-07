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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.services.DeltaTableMetadata
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.{CommandUtils, RunnableCommand}
import org.apache.spark.sql.types.StringType

/**
 * The `vacuum` command implementation for Spark SQL. Example SQL:
 * {{{
 *    VACUUM ('/path/to/dir' | delta.`/path/to/dir`) [RETAIN number HOURS] [DRY RUN];
 * }}}
 */
case class VacuumTableCommand(
    path: Option[String],
    table: Option[TableIdentifier],
    horizonHours: Option[Double],
    dryRun: Boolean,
    autoRun: Boolean = false) extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("path", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val pathToVacuum =
      new Path(if (table.nonEmpty) {
        DeltaTableIdentifier(sparkSession, table.get) match {
          case Some(id) if id.path.isDefined => id.path.get
          case Some(id) => throw DeltaErrors.tableNotSupportedException("VACUUM")
          case None => throw DeltaErrors.notADeltaTableException("VACUUM")
        }
      } else {
        path.get
      })
    val baseDeltaPath = DeltaTableUtils.findDeltaTableRoot(sparkSession, pathToVacuum)
    if (baseDeltaPath.isDefined) {
      if (baseDeltaPath.get != pathToVacuum) {
        throw DeltaErrors.vacuumBasePathMissingException(baseDeltaPath.get)
      }
    }
    val deltaLog = DeltaLog.forTable(sparkSession, pathToVacuum)
    if (deltaLog.snapshot.version == -1) {
      throw DeltaErrors.notADeltaTableException(
        "VACUUM",
        DeltaTableIdentifier(path = Some(pathToVacuum.toString)))
    }
    if (autoRun) {
      if (table.isDefined) {
        logInfo(s"VACUUM ${table.get.unquotedString} AUTO RUN command won't execute immediately")
        updateMetaTable(sparkSession, table.get, pathToVacuum.toString)
        Seq.empty[Row]
      } else {
        throw DeltaErrors.tableOnlySupportedException("VACUUM with AUTO RUN")
      }
    } else {
      val res = VacuumCommand.gc(sparkSession, deltaLog, dryRun, horizonHours).collect()
      table.foreach { t =>
        val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(t)
        CommandUtils.updateTableStats(sparkSession, catalogTable)
      }
      res
    }
  }


  private def updateMetaTable(
      spark: SparkSession, table: TableIdentifier, pathToVacuum: String): Unit = {
    val catalog = spark.sessionState.catalog
    val metadata = DeltaTableMetadata(
      table.database.getOrElse(catalog.getCurrentDatabase),
      table.table,
      catalog.getCurrentUser,
      pathToVacuum,
      vacuum = true,
      horizonHours.getOrElse(7 * 24D).toLong)
    val res = DeltaTableMetadata.updateMetadataTable(spark, metadata)
    if (res) {
      logInfo(s"Update ${table.identifier} in delta metadata table")
    } else {
      logWarning(
        s"""
           |${DeltaSQLConf.META_TABLE_IDENTIFIER.key} may not be created.
           |Skip to store delta metadata to ${DeltaTableMetadata.deltaMetaTableIdentifier(spark)}.
           |This is triggered by command:\n
           |VACUUM ${table.identifier} (RETAIN number HOURS)? AUTO RUN;
           |""".stripMargin)
    }
  }
}
