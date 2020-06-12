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

package org.apache.spark.sql.delta.services

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils}
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.services.DeltaTableMetadata._
import org.apache.spark.sql.delta.services.ui.DeltaTab
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.{CommandUtils, DDLUtils}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.{ThreadUtils, Utils}

class AutoVacuum(ctx: SparkContext) extends Logging {

  private val validate = new ValidateTask(ctx.getConf)

  val started: Boolean = {
    // add delta listener only delta enabled
    if (ctx.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
        .contains("io.delta.sql.DeltaSparkSessionExtension")) {
      ctx.addSparkListener(
        new DeltaTableListener(validate), DeltaTableListener.DELTA_MANAGEMENT_QUEUE)
    }
    // DELTA_AUTO_VACUUM_ENABLED is true only in reserved queue
    if (ctx.getConf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
      val currentQueue = ctx.getConf.get("spark.yarn.queue", "")
      if (!currentQueue.contains("test") && !currentQueue.contains("reserved")) {
        logWarning(s"WARNING!!! Delta auto vacuum function should only enable in reserved queue," +
          s" current queue is $currentQueue. " +
          s"Please set ${DeltaSQLConf.AUTO_VACUUM_ENABLED.key} to false.")
      }

      if (ctx.getConf.get(DeltaSQLConf.AUTO_VACUUM_UI_ENABLED)) {
        ctx.ui.foreach(new DeltaTab(validate, _))
      }

      /**
       * Delta table validation check task from [DELTA_META_TABLE].
       * If we find a new delta table contains auto vacuum settings, add it to vacuumer thread pool.
       * If we find a old delta table is expired, remove it from vacuumer thread pool and
       * delete it from [DELTA_META_TABLE].
       */
      val validatorInterval = ctx.getConf.get(DeltaSQLConf.VALIDATOR_INTERVAL)
      val initialDelay = if (Utils.isTesting) 5 else 300 // 5 min
      val validator = ThreadUtils.newDaemonSingleThreadScheduledExecutor("delta-table-validator")
      validator.scheduleWithFixedDelay(validate, initialDelay, validatorInterval, TimeUnit.SECONDS)
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
    logInfo(s"Start vacuum ${table.identifier.unquotedString} retain ${table.retention} hours")
    VacuumCommand.gc(spark, deltaLog, dryRun = false, Some(table.retention.toDouble),
      safetyCheckEnabled = false)
    CommandUtils.updateTableStats(spark, catalogTable)
    logInfo(s"End vacuum ${table.identifier.unquotedString} retain ${table.retention} hours")
  }
}

/**
 * Validate the table from [[DELTA_META_TABLE_IDENTIFIER]].
 * If it is a new delta table and with vacuum enabled, add it to vacuuming and vacuumPool.
 * If it is an expired delta table (dropped or convert back to parquet),
 * remove it from vacuuming and vacuumPool.
 */
class ValidateTask(conf: SparkConf) extends Runnable with Logging {

  // visible for testing, delta entity -> vacuum task future
  private[sql] lazy val deltaTableToVacuumTask =
    new ConcurrentHashMap[DeltaTableMetadata, Option[ScheduledFuture[_]]]().asScala

  private[sql] var lastUpdatedTime = "Waiting for update"

  private lazy val vacuumPool =
    ThreadUtils.newDaemonFixedThreadScheduledExecutor(32, "delta-auto-vacuum-pool")

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
    if (conf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
      // It has been dropped or is not a delta table any more,
      // we should delete it from DELTA_META_TABLE
      deltaTableToVacuumTask.remove(meta).foreach(_.foreach(_.cancel(true)))
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
    if (conf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
      deltaTableToVacuumTask.get(meta).foreach(_.foreach(_.cancel(true)))
      if (deltaTableToVacuumTask.contains(meta)) {
        deltaTableToVacuumTask(meta) = None
        logInfo(s"Found ${meta.toString}, disable vacuum")
      }
    }
  }

  def enableVacuum(meta: DeltaTableMetadata): Unit = {
    if (conf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
      val interval = conf.get(DeltaSQLConf.AUTO_VACUUM_INTERVAL)
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
    DateTimeUtils.timestampToString(
      DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(System.currentTimeMillis()))) + " " +
      DateTimeUtils.defaultTimeZone().getID
  }
}
