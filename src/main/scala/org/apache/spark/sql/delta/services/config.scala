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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.delta.sources.DeltaSQLConf.buildStaticConf
import org.apache.spark.sql.internal.StaticSQLConf

object config {

  // ---------------------------------------------------- //
  //                    delta manager                     //
  // ---------------------------------------------------- //
  // spark.databricks.delta.*

  val META_TABLE_IDENTIFIER = buildStaticConf("metaTable.identifier")
    .internal
    .doc("Delta meta table identifier: database.table")
    .stringConf
    .createWithDefault("carmel_system.carmel_delta_tables")

  val META_TABLE_STORAGE = buildStaticConf("metaTable.storage")
    .internal
    .doc("Delta meta table storage types: jdbc/spark")
    .stringConf
    .createWithDefault("spark")

  val AUTO_VACUUM_UI_ENABLED = buildStaticConf("vacuum.ui.enabled")
    .booleanConf
    .createWithDefault(true)

  val AUTO_VACUUM_ENABLED = buildStaticConf("vacuum.auto.enabled")
    .doc("If true, it allows to start a thread pool to execute VACUUM periodically. This should " +
      s"only enable in reserved queue and ${StaticSQLConf.SPARK_SESSION_EXTENSIONS.key} set to " +
      "io.delta.sql.DeltaSparkSessionExtension")
    .booleanConf
    .createWithDefault(false)

  val DELTA_LISTENER_ENABLED = buildStaticConf("listener.enabled")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val AUTO_VACUUM_RETENTION_HOURS = buildStaticConf("vacuum.auto.retention.hours")
    .doc("The default Delta tombstone retention period. Should be greater than 0.")
    .longConf
    .checkValue(_ > 0, "Retention hours should be greater than 0")
    .createWithDefault(12L)

  val AUTO_VACUUM_INTERVAL = buildStaticConf("vacuum.schedule.interval")
    .doc("The interval hours of a vacuum task is scheduled. The default value is 24 hours.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(12 * 3600)

  val VALIDATOR_INTERVAL = buildStaticConf("validator.schedule.interval")
    .doc("The interval of delta table validator thread is scheduled.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(30 * 60) // 30 minutes by default

  val DOUBLE_CHECK_INTERVAL = buildStaticConf("doubleChecker.schedule.interval")
    .doc("The interval of delta table double checker thread is scheduled.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(3 * 3600) // 6 hours by default

  val WORKSPACE_BASE_DIR = buildStaticConf("workspace.base.dir")
    .doc("The base directory of workspaces for data uploading.")
    .stringConf
    .createWithDefault("/workspaces")


  // ---------------------------------------------------- //
  //                    mysql (reuse viewpoint)           //
  // ---------------------------------------------------- //
  val SERVER_UI_PORT = ConfigBuilder("spark.viewpoint.ui.port")
    .doc("Web UI port to bind Spark View Point Server")
    .intConf
    .createWithDefault(9090)

  val MYSQL_ENDPOINT = ConfigBuilder("spark.viewpoint.mysql.url")
    .stringConf
    .createWithDefault("jdbc:mysql://mysqlcarmelst.db.stratus.ebay.com:3306/carmelst")

  val MYSQL_USERNAME = ConfigBuilder("spark.viewpoint.mysql.username")
    .stringConf
    .createWithDefault("carmelst")

  val MYSQL_PASSWORD = ConfigBuilder("spark.viewpoint.mysql.password")
    .stringConf
    .createWithDefault("")

  val MYSQL_THREADPOOL_INIT_NUM =
    ConfigBuilder("spark.viewpoint.mysql.threadpool.init.num")
      .intConf
      .createWithDefault(10)

  val MYSQL_THREADPOOL_MAX_NUM =
    ConfigBuilder("spark.viewpoint.mysql.threadpool.max.num")
      .intConf
      .createWithDefault(20)

  val MYSQL_THREADPOOL_MAX_IDLE_TIME =
    ConfigBuilder("spark.viewpoint.mysql.threadpool.max.idle.time")
      .longConf
      .createWithDefault(60 * 1000)
}
