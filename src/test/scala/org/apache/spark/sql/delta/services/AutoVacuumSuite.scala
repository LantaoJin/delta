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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class AutoVacuumSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {

  val DELTA_META_TABLE_NAME = "default.test_carmel_delta_tables"

  val DELTA_META_TABLE_CREATION_SQL =
    s"""
       |CREATE TABLE IF NOT EXISTS $DELTA_META_TABLE_NAME
       | (`db` STRING, `tbl` STRING, `maker` STRING COMMENT 'Who convert a table to delta
       | or create a delta table. In most cases, it is table owner.', `path` STRING,
       | `vacuum` BOOLEAN COMMENT 'If true, vacuums the table by clearing all untracked files.',
       | `retention` BIGINT COMMENT 'Tombstones over this retention will be dropped
       | and files will be deleted.')
       | USING parquet
       |""".stripMargin

  test("test auto vacuum") {
    sys.props("spark.testing") = "true"
    withTempDir { f =>
      spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, true)
      val path = f.getAbsolutePath
      spark.sessionState.conf.setConf(DeltaSQLConf.META_TABLE_LOCATION, Some(path))
      withTable(s"$DELTA_META_TABLE_NAME") {
        sql(
          s"""
             |${DELTA_META_TABLE_CREATION_SQL}
             |LOCATION "file:$path"
             |""".stripMargin)
        sql(
          s"""
             |INSERT INTO TABLE $DELTA_META_TABLE_NAME
             |VALUES ('db1', 'tbl1', 'user1', 'path1', false, -1)
             |""".stripMargin)
        sql(
          s"""
             |INSERT INTO TABLE $DELTA_META_TABLE_NAME
             |VALUES ('default', 'tbl', 'user', 'path', true, 24)
             |""".stripMargin)
        sql(
          s"""
            |CONVERT TO DELTA $DELTA_META_TABLE_NAME
            |""".stripMargin)
        val mgr = new AutoVacuum(spark.sparkContext)
        Thread.sleep(2000) // wait vacuum threads completed
        val all = mgr.vacuuming.keys.toSeq
        // only store the records which field vacuum is true
        assert(
          !all.contains(DeltaTableMetadata("db1", "tbl1", "user1", "path1", false, Some(-1))))
        assert(
          all.contains(DeltaTableMetadata("default", "tbl", "user", "path", true, Some(24))))
        mgr.stopAll()
      }
    }
  }
}
