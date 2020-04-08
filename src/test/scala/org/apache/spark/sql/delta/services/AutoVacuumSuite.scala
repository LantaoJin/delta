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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.datasources.IndexUtils
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class AutoVacuumSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {

  override def afterEach(): Unit = {
    super.afterEach()
    sys.props("spark.testing") = "false"
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, false)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_CRUD_ASYNC, false)
  }

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

  test("test auto vacuum and show deltas") {
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_CRUD_ASYNC, false)
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, true)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    withTempDir{ dir =>
      withTable(s"$DELTA_META_TABLE_NAME") {
        sql(
          s"""
             |${DELTA_META_TABLE_CREATION_SQL}
             |LOCATION '$dir'
             |""".stripMargin)
        sql(
          s"""
             |INSERT INTO TABLE $DELTA_META_TABLE_NAME
             |VALUES ('db1', 'tbl1', 'user1', 'path1', false, 7 * 24)
             |""".stripMargin)
        sql(
          s"""
             |INSERT INTO TABLE $DELTA_META_TABLE_NAME
             |VALUES ('default', 'tbl', 'user', 'path', true, 24)
             |""".stripMargin)
        val mgr = new AutoVacuum(spark.sparkContext)
        sql(
          s"""
             |CONVERT TO DELTA $DELTA_META_TABLE_NAME
             |""".stripMargin)
        Thread.sleep(2000) // wait vacuum threads completed
        val all = mgr.vacuuming.keys.toSeq
        // only store the records which field vacuum is true
        assert(
          !all.contains(DeltaTableMetadata("db1", "tbl1", "user1", "path1", false, 168L)))
        assert(
          all.contains(DeltaTableMetadata("default", "tbl", "user", "path", true, 24L)))
        mgr.stopAll()

        checkAnswer(
          sql("SHOW DELTAS"),
          Seq(
            Row("db1", "tbl1", "user1", "path1", false, 168L),
            Row("default", "tbl", "user", "path", true, 24L),
            Row("default", "test_carmel_delta_tables", "",
              s"${IndexUtils.removeEndPathSep(dir.toURI.toString)}", false, 168L))
        )

        sql(s"VACUUM $DELTA_META_TABLE_NAME RETAIN 200 HOURS AUTO RUN")
        sql("SHOW DELTAS").show()
        checkAnswer(
          sql("SHOW DELTAS"),
          Seq(
            Row("db1", "tbl1", "user1", "path1", false, 168L),
            Row("default", "tbl", "user", "path", true, 24L),
            Row("default", "test_carmel_delta_tables", "",
              s"${IndexUtils.removeEndPathSep(dir.toURI.toString)}", true, 200L))
        )
      }
    }
  }

  test("test auto vacuum and show deltas async") {
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_CRUD_ASYNC, true)
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, true)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    withTempDir{ dir =>
      withTable(s"$DELTA_META_TABLE_NAME") {
        sql(
          s"""
             |${DELTA_META_TABLE_CREATION_SQL}
             |LOCATION '$dir'
             |""".stripMargin)
        sql(
          s"""
             |INSERT INTO TABLE $DELTA_META_TABLE_NAME
             |VALUES ('db1', 'tbl1', 'user1', 'path1', false, 7 * 24)
             |""".stripMargin)
        sql(
          s"""
             |INSERT INTO TABLE $DELTA_META_TABLE_NAME
             |VALUES ('default', 'tbl', 'user', 'path', true, 24)
             |""".stripMargin)
        spark.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
          "io.delta.sql.DeltaSparkSessionExtension")
        val mgr = new AutoVacuum(spark.sparkContext)
        sql(
          s"""
             |CONVERT TO DELTA $DELTA_META_TABLE_NAME
             |""".stripMargin)
        Thread.sleep(2000) // wait vacuuming threads completed
        val all = mgr.vacuuming.keys.toSeq
        // only store the records which field vacuum is true
        assert(
          !all.contains(DeltaTableMetadata("db1", "tbl1", "user1", "path1", false, 168L)))
        assert(
          all.contains(DeltaTableMetadata("default", "tbl", "user", "path", true, 24L)))
        mgr.stopAll()

        checkAnswer(
          sql("SHOW DELTAS"),
          Seq(
            Row("db1", "tbl1", "user1", "path1", false, 168L),
            Row("default", "tbl", "user", "path", true, 24L),
            Row("default", "test_carmel_delta_tables", "",
              s"${IndexUtils.removeEndPathSep(dir.toURI.toString)}", false, 168L))
        )

        sql(s"VACUUM $DELTA_META_TABLE_NAME RETAIN 200 HOURS AUTO RUN")
        Thread.sleep(1000) // wait updating META_TABLE async
        sql("SHOW DELTAS").show()
        checkAnswer(
          sql("SHOW DELTAS"),
          Seq(
            Row("db1", "tbl1", "user1", "path1", false, 168L),
            Row("default", "tbl", "user", "path", true, 24L),
            Row("default", "test_carmel_delta_tables", "",
              s"${IndexUtils.removeEndPathSep(dir.toURI.toString)}", true, 200L))
        )
      }
    }
  }

  test("test rename and drop delta for metadata table") {
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
      "io.delta.sql.DeltaSparkSessionExtension")
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_CRUD_ASYNC, true)
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, false)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    withTempPaths(numPaths = 2) { case Seq(dir, dir1) =>
      withTable(s"$DELTA_META_TABLE_NAME") {
        sql(
          s"""
             |${DELTA_META_TABLE_CREATION_SQL}
             |LOCATION '$dir'
             |""".stripMargin)
        sql(
          s"""
             |CONVERT TO DELTA $DELTA_META_TABLE_NAME
             |""".stripMargin)
        sql(
          s"""
            |CREATE TABLE delta1(id INT) USING parquet
            |LOCATION '$dir1'
            |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA delta1
            |""".stripMargin)
        Thread.sleep(3000)
        checkAnswer(
          sql("SHOW DELTAS"),
          Row("default", "delta1", "",
            s"${IndexUtils.removeEndPathSep(dir1.toURI.toString)}", false, 168L) ::
          Row("default", "test_carmel_delta_tables", "",
            s"${IndexUtils.removeEndPathSep(dir.toURI.toString)}", false, 168L) :: Nil)
        sql(
          """
            |ALTER TABLE delta1 RENAME TO delta2
            |""".stripMargin)
        Thread.sleep(3000)
        checkAnswer(
          sql("SHOW DELTAS"),
          Row("default", "delta2", "",
            s"${IndexUtils.removeEndPathSep(dir1.toURI.toString)}", false, 168L) ::
          Row("default", "test_carmel_delta_tables", "",
            s"${IndexUtils.removeEndPathSep(dir.toURI.toString)}", false, 168L) :: Nil)
      }
    }
  }
}
