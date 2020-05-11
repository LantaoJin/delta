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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.commands.{DeltaCommand, VacuumCommand}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.{deltaFile, isDeltaFile}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.{CommandUtils, RunnableCommand}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric

/**
 * Rollback a delta table to specified version
 */
case class RollbackCommand(
    tableName: TableIdentifier,
    timestampOpt: Option[String],
    versionOpt: Option[Long]) extends RunnableCommand with DeltaCommand {

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numRemovedVersions" -> createMetric(sc, "number of delta Version files removed."),
    "numRemovedCheckpoints" -> createMetric(sc, "number of Checkpoint files removed.")
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    if (!DeltaTableUtils.isDeltaTable(table)) {
      throw DeltaErrors.timeTravelNotSupportedException
    }
    val path =
      new Path(DeltaTableIdentifier(sparkSession, tableName) match {
        case Some(id) if id.path.isDefined => id.path.get
        case Some(_) => throw DeltaErrors.tableNotSupportedException("ROLLBACK")
        case None => throw DeltaErrors.notADeltaTableException("ROLLBACK")
      })
    val deltaLog = DeltaLog.forTable(sparkSession, path)

    recordDeltaOperation(deltaLog, "delta.dml.rollback") {
      deltaLog.withNewTransaction { txn =>
        performRollback(sparkSession, deltaLog, txn)
      }
    }

//    // todo(lajin)
//    vacuum(sparkSession, deltaLog, table)

    Seq.empty[Row]
  }

  private def performRollback(
      sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction) = {
    val store = deltaLog.store
    val logPath = deltaLog.logPath
    deltaLog.startTransaction()

    if (versionOpt.isDefined) {
      deltaLog.lockInterruptibly {
        val version = versionOpt.get

        // delete all version files which their version number is greater than
        // the version we need to rollback
        val deltaLogsToDelete = store.listFrom(deltaFile(logPath, version + 1))
          .map(_.getPath).filter(isDeltaFile).toArray
        // delete all checkpoint files which their version number is greater than
        // the version we need to rollback
        val checkpointsToDelete = store.listFrom(deltaFile(logPath, version + 1))
          .map(_.getPath).filter(FileNames.isCheckpointFile).toArray
        if (deltaLogsToDelete.isEmpty) {
          throw DeltaErrors.rollbackToInvalidVersion(version)
        }
        logInfo(s"To rollback to version $version:\n"
          + s"Delete version files ${deltaLogsToDelete.mkString(", ")}, \n"
          + s"Delete checkpoint files ${checkpointsToDelete.mkString(", ")}")

        // delete them
        store.delete(deltaLogsToDelete)
        metrics("numRemovedVersions").set(deltaLogsToDelete.length)
        if (checkpointsToDelete.length > 0) {
          store.delete(checkpointsToDelete)
        }
        metrics("numRemovedCheckpoints").set(checkpointsToDelete.length)

        val snapshot =
          try {
            deltaLog.getSnapshotAt(version)
          } catch {
            case e: FileNotFoundException =>
              throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(e)
          }
        // checkpoint to update _last_checkpoint
        deltaLog.setCurrentSnapshot(snapshot)
        deltaLog.checkpoint(overwrite = true)

        val currentSnapshot = deltaLog.update()
        assert(currentSnapshot.version == snapshot.version,
          "currentSnapshot changed after deltaLog.update() in Rollback procession: "
            + s"before ${snapshot.version}, after ${currentSnapshot.version}")
      }
    } else {
      // todo(lajin)
      throw DeltaErrors.operationNotSupportedException("ROLLBACK AT timestamp")
    }
    txn.registerSQLMetrics(sparkSession, metrics)
    // This is needed to make the SQL metrics visible in the Spark UI
    val executionId = sparkSession.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkSession.sparkContext, executionId, metrics.values.toSeq)
  }

  private def vacuum(sparkSession: SparkSession, deltaLog: DeltaLog, table: CatalogTable): Unit = {
    // vacuum related untracked files
    val oldCheckEnabledValue =
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED)
    // disable retention check
    sparkSession.sessionState.conf.setConf(
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED, false)
    VacuumCommand.gc(sparkSession, deltaLog, dryRun = false, Some(0))
    CommandUtils.updateTableStats(sparkSession, table)
    sparkSession.sessionState.conf.setConf(
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED, oldCheckEnabledValue)
  }
}
