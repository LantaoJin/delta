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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.services.DeltaTableMetadata._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Vacuum a delta table periodically.
 */
class VacuumTask(table: DeltaTableMetadata) extends Runnable {
  private val spark = SparkSession.getDefaultSession.get

  override def run(): Unit = {
    val sqlText = s"VACUUM ${table.identifier} RETAIN ${table.retention} HOURS"
    val plan = spark.sessionState.sqlParser.parsePlan(sqlText)
    val qe = new QueryExecution(spark, plan, withAuth = false)
    qe.assertAnalyzed()
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
  import spark.implicits._

  // visible for testing, vacuum entity -> future
  private[sql] lazy val vacuuming =
    new ConcurrentHashMap[DeltaTableMetadata, ScheduledFuture[_]]().asScala

  private lazy val vacuumPool =
    ThreadUtils.newDaemonFixedThreadScheduledExecutor(16, "delta-auto-vacuum-pool")

  override def run(): Unit = {
    createDataFrameOfDeltaMetaTable(spark).foreach { df =>
      df.as[DeltaTableMetadata].collect().foreach { meta =>
        if (DeltaTableUtils.isDeltaTable(spark, meta.identifier) || Utils.isTesting) {
          if (vacuuming.contains(meta)) {
            if (meta.vacuum && meta.retention.getOrElse(-1L) > 0L) {
              // just logging
              logDebug(s"Found ${meta.toString}, vacuuming in progressing")
            } else {
              invalidate(meta)
            }
          } else {
            if (meta.vacuum && meta.retention.getOrElse(-1L) > 0L) {
              if (conf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
                val interval = conf.get(DeltaSQLConf.AUTO_VACUUM_INTERVAL)
                val vacuumTask = new VacuumTask(meta)
                val schedule = vacuumPool.scheduleWithFixedDelay(
                  vacuumTask, interval, interval, TimeUnit.SECONDS)
                vacuuming(meta) = schedule
                logInfo(s"Found ${meta.toString}, vacuum it in $interval seconds")
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
  }

  private def invalidate(meta: DeltaTableMetadata): Unit = {
    // try to remove from vacuuming list
    vacuuming.remove(meta).foreach(_.cancel(true))
    // It has been dropped or is not a delta table any more,
    // we should delete it from DELTA_META_TABLE
    val deltaMetaTable = deltaMetaTableIdentifier(conf)
    if (deleteWithCondition(spark, meta.db, meta.tbl)) {
      logInfo(s"Deleted expired ${meta.toString} from $deltaMetaTable")
    } else {
      logWarning(s"Failed to delete expired ${meta.toString} from $deltaMetaTable")
    }
  }
}

class AutoVacuum(ctx: SparkContext) extends Logging {

  private lazy val validate = new ValidateTask(ctx.conf)

  val started: Boolean = {
    // DELTA_AUTO_VACUUM_ENABLED is true only in reserved queue
    if (ctx.conf.get(DeltaSQLConf.AUTO_VACUUM_ENABLED)) {
      val currentQueue = ctx.conf.get("spark.yarn.queue", "")
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
      val validatorInterval = ctx.conf.get(DeltaSQLConf.VALIDATOR_INTERVAL)
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
