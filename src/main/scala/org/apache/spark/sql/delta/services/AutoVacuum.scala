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

package org.apache.spark.sql.delta.services

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils}
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.services.DeltaTableMetadata._
import org.apache.spark.sql.delta.services.ui.{DeltaTab, VacuumSkipped, VacuumingInfo}
import org.apache.spark.sql.execution.command.{CommandUtils, DDLUtils}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.{ThreadUtils, Utils}

class AutoVacuum(ctx: SparkContext) extends Logging {

  private val validate = new ValidateTask(ctx.getConf)

  private lazy val validator =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("delta-table-validator")

  private lazy val doubleCheckerThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("delta-double-checker")

  val started: Boolean = {
    // add delta listener only delta enabled
    if (ctx.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty)
        .contains(classOf[DeltaSparkSessionExtension].getName) &&
        ctx.getConf.get(config.DELTA_LISTENER_ENABLED)) {
      ctx.addSparkListener(
        new DeltaTableListener(validate), DeltaTableListener.DELTA_MANAGEMENT_QUEUE)
    }
    // DELTA_AUTO_VACUUM_ENABLED is true only in reserved queue
    if (ctx.getConf.get(config.AUTO_VACUUM_ENABLED)) {
      val currentQueue = ctx.getConf.get("spark.yarn.queue", "")
      if (!currentQueue.contains("test") && !currentQueue.contains("reserved")) {
        logWarning(s"WARNING!!! Delta auto vacuum function should only enable in reserved queue," +
          s" current queue is $currentQueue. " +
          s"Please set ${config.AUTO_VACUUM_ENABLED.key} to false.")
      }

      if (ctx.getConf.get(config.AUTO_VACUUM_UI_ENABLED)) {
        ctx.ui.foreach(new DeltaTab(validate, _))
      }

      /**
       * Delta table validation check task from [DELTA_META_TABLE].
       * If we find a new delta table contains auto vacuum settings, add it to vacuumer thread pool.
       * If we find a old delta table is expired, remove it from vacuumer thread pool and
       * delete it from [DELTA_META_TABLE].
       */
      val validatorInterval = ctx.getConf.get(config.VALIDATOR_INTERVAL)
      val initialDelay = if (Utils.isTesting) 5 else 300 // 5 min
      validator.scheduleWithFixedDelay(validate, initialDelay, validatorInterval, TimeUnit.SECONDS)
      val doubleCheckerInterval = ctx.getConf.get(config.DOUBLE_CHECK_INTERVAL)
      doubleCheckerThread.scheduleWithFixedDelay(new DoubleCheckerTask(ctx.getConf, validate),
        if (Utils.isTesting) 15 else doubleCheckerInterval, doubleCheckerInterval, TimeUnit.SECONDS)
      true
    } else {
      false
    }
  }

  private[sql] def deltaTableToVacuumTask
      : mutable.Map[DeltaTableMetadata, Option[ScheduledFuture[_]]] = {
    validate.deltaTableToVacuumTask
  }

  def stopAll(): Unit = {
    deltaTableToVacuumTask.values.foreach(_.foreach(_.cancel(true)))
    deltaTableToVacuumTask.clear()
  }
}

/**
 * Validate the table from [[DELTA_META_TABLE_IDENTIFIER]].
 * If it is a new delta table and with vacuum enabled, add it to vacuuming and vacuumPool.
 * If it is an expired delta table (dropped or convert back to parquet),
 * remove it from vacuuming and vacuumPool.
 */
class ValidateTask(conf: SparkConf) extends Runnable with Logging {

  private val DDL_TIME = "transient_lastDdlTime"

  // visible for testing, delta entity -> vacuum task future
  private[sql] lazy val deltaTableToVacuumTask =
    new ConcurrentHashMap[DeltaTableMetadata, Option[ScheduledFuture[_]]]().asScala

  private[sql] lazy val vacuumHistory =
    new ConcurrentHashMap[DeltaTableMetadata, VacuumingInfo]().asScala

  private[sql] var lastUpdatedTime = "Waiting for update"

  private lazy val vacuumPool =
    ThreadUtils.newDaemonThreadPoolScheduledExecutor("delta-auto-vacuum-pool", 32)

  private[sql] lazy val skipping =
    new ConcurrentHashMap[DeltaTableMetadata, VacuumSkipped]().asScala

  override def run(): Unit = {
    val spark = SparkSession.active
    val existInDb = mutable.Set.empty[DeltaTableMetadata]
    listMetadataTables(spark).foreach { meta =>
      existInDb.add(meta)
      if (DeltaTableUtils.isDeltaTable(spark, meta.identifier) || Utils.isTesting) {
        if (deltaTableToVacuumTask.contains(meta)) { // delta table in our management
          if (deltaTableToVacuumTask(meta).isDefined) { // delta in vacuuming
            if (meta.vacuum && meta.retention > 0L) {
              getOldTableMeta(meta) match {
                case Some(old) =>
                  if (old.retention != meta.retention) { // re-vacuum if retention changed
                    disableVacuum(old)
                    enableVacuum(meta)
                  } else {
                    // just logging
                    logInfo(s"Found ${old.toString} in vacuum backend thread")
                  }
                case None =>
              }
            } else {
              disableVacuum(meta)
            }
          } else {
            if (meta.vacuum && meta.retention > 0L) {
              enableVacuum(meta)
            } else {
              logInfo(s"Found ${meta.toString}, no need to vacuum")
            }
          }
        } else { // delta is not in management - new delta table
          if (meta.vacuum && meta.retention > 0L) {
            enableVacuum(meta)
          } else {
            deltaTableToVacuumTask(meta) = None
            logInfo(s"Found ${meta.toString}, no need to vacuum")
          }
        }
      } else {
        invalidate(spark, meta)
      }
    }

    // remove metas which only exist in memory
    val existInMemory = deltaTableToVacuumTask.keySet
    val shouldToRemove = existInMemory.diff(existInDb)
    shouldToRemove.foreach(invalidate(spark, _, deleteDb = false))

    lastUpdatedTime = getCurrentTimestampString
  }

  def getOldTableMeta(search: DeltaTableMetadata): Option[DeltaTableMetadata] = {
    deltaTableToVacuumTask.keySet.find(_.equals(search))
  }

  def invalidate(spark: SparkSession, meta: DeltaTableMetadata, deleteDb: Boolean = true): Unit = {
    if (conf.get(config.AUTO_VACUUM_ENABLED)) {
      // It has been dropped or is not a delta table any more,
      // we should delete it from DELTA_META_TABLE
      deltaTableToVacuumTask.remove(meta).foreach(_.foreach(_.cancel(true)))
      vacuumHistory.remove(meta)
      if (deleteDb) {
        val deltaMetaTable = deltaMetaTableIdentifier(conf)
        if (deleteFromMetadataTable(spark, meta)) {
          logInfo(s"Deleted expired ${meta.toString} from $deltaMetaTable")
        } else {
          logWarning(s"Failed to delete expired ${meta.toString} from $deltaMetaTable")
        }
      }
    }
  }

  def disableVacuum(meta: DeltaTableMetadata): Unit = {
    if (conf.get(config.AUTO_VACUUM_ENABLED)) {
      deltaTableToVacuumTask.get(meta).foreach(_.foreach(_.cancel(true)))
      if (deltaTableToVacuumTask.contains(meta)) {
        deltaTableToVacuumTask(meta) = None
        logInfo(s"Found ${meta.toString}, disable vacuum")
      }
    }
  }

  def enableVacuum(meta: DeltaTableMetadata): Unit = {
    if (conf.get(config.AUTO_VACUUM_ENABLED) && meta.vacuum) {
      val interval = conf.get(config.AUTO_VACUUM_INTERVAL)
      val vacuumTask = new VacuumTask(meta)
      // 1 hour + random in 12 hours by default
      val initialDelay = 3600 + Random.nextInt((interval / 2).toInt)
      val schedule = vacuumPool.scheduleWithFixedDelay(
        vacuumTask, initialDelay, interval, TimeUnit.SECONDS)
      deltaTableToVacuumTask(meta) = Some(schedule)
      logInfo(s"Found ${meta.toString}, enable vacuum in $initialDelay seconds")
    }
  }

  def getCurrentTimestampString: String = {
    val tf = TimestampFormatter.getFractionFormatter(DateTimeUtils.defaultTimeZone.toZoneId)
    DateTimeUtils.timestampToString(tf,
      DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(System.currentTimeMillis()))) + " " +
      DateTimeUtils.defaultTimeZone().getID
  }

  /**
   * Vacuum a delta table periodically.
   */
  class VacuumTask(table: DeltaTableMetadata) extends Runnable with Logging {
    private val spark = SparkSession.getDefaultSession.get

    override def run(): Unit = {
      val catalogTable = spark.sessionState.catalog.getTableMetadata(table.identifier)
      if (DDLUtils.isTemporaryTable(catalogTable)) {
        logInfo(s"Skip vacuum temporary table ${table.identifier.unquotedString}")
        return
      }
      val deltaLog = DeltaLog.forTable(spark, catalogTable)

      val start = getCurrentTimestampString
      val currentLastDDLTime = catalogTable.properties(DDL_TIME).toLong
      val deleteBeforeTimestamp =
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(table.retention)
      if (deltaLog.snapshot.tombstones
          .filter(r => r.delTimestamp < deleteBeforeTimestamp).count() > 0) {
        // only vacuum the tables changed between twice vacuuming or first look
        if (vacuumHistory.get(table).exists(_.lastDDLTime == currentLastDDLTime)) {
          val reason = s"lastDDLTime $currentLastDDLTime not changed"
          logInfo(s"Skip vacuum table ${table.identifier.unquotedString} $reason")
          updateSkippedHistory(start, reason)
        } else {
          val vacuumingInfoBefore = VacuumingInfo(table.db, table.tbl, -1L, -1L,
            start, null, currentLastDDLTime)
          vacuumHistory(table) = vacuumingInfoBefore

          logInfo(s"Start vacuum ${table.identifier.unquotedString} " +
            s"retain ${table.retention} hours")
          val (_, filesDeleted, fileCounts) = VacuumCommand.gc(spark, deltaLog, dryRun = false,
            Some(table.retention.toDouble), safetyCheckEnabled = false)
          CommandUtils.updateTableStats(spark, catalogTable)
          logInfo(s"End vacuum ${table.identifier.unquotedString} " +
            s"retain ${table.retention} hours")
          val end = getCurrentTimestampString
          val vacuumingInfoAfter = VacuumingInfo(table.db, table.tbl, fileCounts, filesDeleted,
            start, end, currentLastDDLTime)
          vacuumHistory(table) = vacuumingInfoAfter
        }
      } else {
        // todo try run for testing, this should be removed in future
        val (_, toBeDeleted, fileCounts) = VacuumCommand.gc(spark, deltaLog, dryRun = true,
          Some(table.retention.toDouble), safetyCheckEnabled = false)
        if (toBeDeleted > 0) {
          val reason = s"Found $toBeDeleted files to be deleted"
          updateSkippedHistory(start, reason, fileCounts)
          logError(s"Invalid skipping vacuum table ${table.identifier.unquotedString}, $reason")
        } else {
          updateSkippedHistory(start, "Table without tombstones", fileCounts)
          logInfo(s"Skip vacuum table ${table.identifier.unquotedString} without tombstones")
        }
      }
    }

    private def updateSkippedHistory(start: String, reason: String, fileCounts: Long = -1L) = {
      skipping.get(table) match {
        case Some(old) => old.update(fileCounts, start, reason)
        case None => skipping(table) = VacuumSkipped(table.db, table.tbl, fileCounts, start, reason)
      }
    }
  }
}

/**
 * Double check the table is delta table and add it to the delta meta table
 */
class DoubleCheckerTask(conf: SparkConf, validate: ValidateTask) extends Runnable with Logging {

  override def run(): Unit = {
    val spark = SparkSession.active
    val catalog = spark.sessionState.catalog
    val baseDir = conf.get(config.WORKSPACE_BASE_DIR)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val basePath = new Path(baseDir)
    val fs = basePath.getFileSystem(hadoopConf)
    try {
      fs.listStatus(basePath)
        .filter(_.isDirectory)
        .map(_.getPath.getName.toLowerCase())
        .filter(_.startsWith("p_"))
        .filter(_.endsWith("_t")).foreach { db =>
        logInfo(s"Begin to double check the delta tables in $db")
        try {
          catalog.getTablesByName(catalog.listTables(db))
            .filter(DDLUtils.isDeltaTable).filterNot(DDLUtils.isTemporaryTable).foreach { table =>
            try {
              val defaultRetentionHours = conf.get(config.AUTO_VACUUM_RETENTION_HOURS)
              val metadata = DeltaTableMetadata(
                table.identifier.database.getOrElse(""),
                table.identifier.table,
                table.owner,
                CatalogUtils.URIToString(table.location),
                vacuum = true,
                retention = defaultRetentionHours)

              if (!DeltaTableMetadata.metadataTableExists(spark, metadata)) {
                logInfo(s"Found $metadata is missing in list, insert into meta table.")
                validate.enableVacuum(metadata)
                DeltaTableMetadata.insertIntoMetadataTable(spark, metadata)
              }
            } catch {
              case e: Throwable =>
                logWarning(s"Catch an exception when double check table ${table.identifier}", e)
            }
          }
        } catch {
          case e: Throwable =>
            logWarning(s"Catch an exception when double check db $db", e)
        }
      }
    } catch {
      case e: Throwable =>
        logWarning("Catch an exception when double check", e)
    } finally {
      fs.close()
    }
  }
}
