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
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils}
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.services.DeltaTableMetadata._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.{ThreadUtils, Utils}

class AutoVacuum(ctx: SparkContext) extends Logging {

  private lazy val validate = new ValidateTask(ctx.getConf)

  val started: Boolean = {
    // add delta listener only delta enabled
    if (ctx.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
        .contains("io.delta.sql.DeltaSparkSessionExtension")) {
      ctx.addSparkListener(new DeltaTableListener, DeltaTableListener.DELTA_MANAGEMENT_QUEUE)
    }
    // DELTA_AUTO_VACUUM_ENABLED is true only in reserved queue
    if (ctx.getConf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
      val currentQueue = ctx.getConf.get("spark.yarn.queue", "")
      if (!currentQueue.contains("test") || !currentQueue.contains("reserved")) {
        logWarning(s"WARNING!!! Delta auto vacuum function should only enable in reserved queue," +
          s" current queue is $currentQueue. " +
          s"Please set ${DeltaSQLConf.AUTO_VACUUM_ENABLED.key} to false.")
      }

      /**
       * Delta table validation check task from [DELTA_META_TABLE].
       * If we find a new delta table contains auto vacuum settings, add it to vacuumer thread pool.
       * If we find a old delta table is expired, remove it from vacuumer thread pool and
       * delete it from [DELTA_META_TABLE].
       */
      val validatorInterval = ctx.getConf.get(DeltaSQLConf.VALIDATOR_INTERVAL)
      val initialDelay = if (Utils.isTesting) 0 else 300 // 5 min
      val validator = ThreadUtils.newDaemonSingleThreadScheduledExecutor("delta-table-validator")
      validator.scheduleWithFixedDelay(validate, initialDelay, validatorInterval, TimeUnit.SECONDS)
      true
    } else {
      false
    }
  }

  private[sql] def vacuuming: mutable.Map[DeltaTableMetadata, ScheduledFuture[_]] = {
    validate.vacuuming
  }

  def stopAll(): Unit = {
    vacuuming.values.foreach(_.cancel(true))
    vacuuming.clear()
  }
}

/**
 * Vacuum a delta table periodically.
 */
class VacuumTask(table: DeltaTableMetadata) extends Runnable with Logging {
  private val spark = SparkSession.getDefaultSession.get

  override def run(): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(table.identifier)
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

  private val spark = SparkSession.getDefaultSession.get

  // visible for testing, vacuum entity -> future
  private[sql] lazy val vacuuming =
    new ConcurrentHashMap[DeltaTableMetadata, ScheduledFuture[_]]().asScala

  private lazy val vacuumPool =
    ThreadUtils.newDaemonFixedThreadScheduledExecutor(16, "delta-auto-vacuum-pool")

  override def run(): Unit = {
    listMetadataTables(spark).foreach { meta =>
      if (DeltaTableUtils.isDeltaTable(spark, meta.identifier) || Utils.isTesting) {
        if (vacuuming.contains(meta)) {
          if (meta.vacuum) {
            // just logging
            logInfo(s"Found ${meta.toString} in vacuum backend thread")
          } else {
            invalidate(meta)
          }
        } else {
          if (meta.vacuum && meta.retention >= 0L) {
            if (conf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
              val interval = conf.get(DeltaSQLConf.AUTO_VACUUM_INTERVAL)
              val vacuumTask = new VacuumTask(meta)
              val initialDelay = interval / 2 + Random.nextInt(600) // 12 hours + 10 minutes
              val schedule = vacuumPool.scheduleWithFixedDelay(
                vacuumTask, initialDelay, interval, TimeUnit.SECONDS)
              vacuuming(meta) = schedule
              logInfo(s"Found ${meta.toString}, will vacuum it in $initialDelay seconds")
            } else { // just logging
              logDebug(s"Found ${meta.toString}, " +
                s"but ${DeltaSQLConf.AUTO_VACUUM_INTERVAL.key} is false")
            }
          } else { // just logging
            logInfo(s"Found ${meta.toString}, no need to vacuum")
          }
        }
      } else {
        invalidate(meta)
      }
    }
  }

  private def invalidate(meta: DeltaTableMetadata): Unit = {
    // try to remove from vacuuming list
    vacuuming.remove(meta).foreach(_.cancel(true))
    // It has been dropped or is not a delta table any more,
    // we should delete it from DELTA_META_TABLE
    val deltaMetaTable = deltaMetaTableIdentifier(conf)
    if (deleteFromMetadataTable(spark, meta)) {
      logInfo(s"Deleted expired ${meta.toString} from $deltaMetaTable")
    } else {
      logWarning(s"Failed to delete expired ${meta.toString} from $deltaMetaTable")
    }
  }
}
