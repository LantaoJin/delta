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

import java.sql.{DriverManager, SQLException}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.util.Utils

class AutoVacuumSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {

  override def afterEach(): Unit = {
    super.afterEach()
    sys.props("spark.testing") = "false"
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, false)
  }

  val DELTA_META_TABLE_NAME = "carmel_system.carmel_delta_tables"

  val DELTA_META_TABLE_DROP_SQL =
    s"""
       |DROP TABLE $DELTA_META_TABLE_NAME
       |""".stripMargin

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
  val DELTA_META_TABLE_CREATION_SQL2 =
    s"""
       |CREATE TABLE $DELTA_META_TABLE_NAME
       |(
       |    db        VARCHAR(100) NOT NULL,
       |    tbl       VARCHAR(200) NOT NULL,
       |    maker     VARCHAR(100),
       |    path      VARCHAR(1000),
       |    vacuum    BOOLEAN,
       |    retention BIGINT,
       |    PRIMARY KEY (db, tbl)
       |)
       |""".stripMargin

  def getTableLocation(tableName: String, db: Option[String] = None): String = {
    CatalogUtils.URIToString(
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, db)).location)
  }

  test("test auto vacuum and show deltas") {
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, true)
    spark.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
      "io.delta.sql.DeltaSparkSessionExtension")
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    withDatabase("carmel_system") {
      sql("create database carmel_system")
      sql("use carmel_system")
      withTempDir { dir =>
        withTable(s"$DELTA_META_TABLE_NAME") {
          sql(
            s"""
               |${DELTA_META_TABLE_CREATION_SQL}
               |LOCATION '$dir'
               |""".stripMargin)
          sql(
            s"""
               |INSERT INTO TABLE $DELTA_META_TABLE_NAME
               |VALUES ('db1', 'tbl1', 'user1', 'path1', false, 14 * 24)
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
          Thread.sleep(5000) // wait vacuum threads completed
          val vmMgr = spark.sharedState.vacuumManager
          val all = vmMgr.deltaTableToVacuumTask
          assert(all.contains(DeltaTableMetadata("db1", "tbl1", "user1", "path1", false, 168L)))
          assert(all(DeltaTableMetadata("db1", "tbl1", "user1", "path1", false, 168L)).isEmpty)
          assert(all.contains(DeltaTableMetadata("default", "tbl", "user", "path", true, 24L)))
          checkAnswer(
            sql("SHOW DELTAS"),
            Seq(
              Row("db1", "tbl1", "user1", "path1", false, 336L),
              Row("default", "tbl", "user", "path", true, 24L),
              Row("carmel_system", "carmel_delta_tables", "",
                s"${getTableLocation("carmel_delta_tables")}", true, 2L))
          )

          sql(s"VACUUM $DELTA_META_TABLE_NAME SET RETAIN 200 HOURS AUTO RUN")
          Thread.sleep(3000)
          checkAnswer(
            sql("SHOW DELTAS"),
            Seq(
              Row("db1", "tbl1", "user1", "path1", false, 336L),
              Row("default", "tbl", "user", "path", true, 24L),
              Row("carmel_system", "carmel_delta_tables", "",
                s"${getTableLocation("carmel_delta_tables")}", true, 200L))
          )
          vmMgr.stopAll()
        }
      }
    }
  }

  test("test rename and drop delta for metadata table") {
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
      "io.delta.sql.DeltaSparkSessionExtension")
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, true)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    withDatabase("carmel_system") {
      sql("create database carmel_system")
      sql("use carmel_system")
      withTempPaths(numPaths = 2) { case Seq(dir, dir1) =>
        withTable(s"$DELTA_META_TABLE_NAME", "delta1", "delta2") {
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
            Row("carmel_system", "delta1", "",
              s"${getTableLocation("delta1")}", true, 2L) ::
              Row("carmel_system", "carmel_delta_tables", "",
                s"${getTableLocation("carmel_delta_tables")}", true, 2L) :: Nil)
          sql(
            """
              |ALTER TABLE delta1 RENAME TO delta2
              |""".stripMargin)
          Thread.sleep(3000)
          checkAnswer(
            sql("SHOW DELTAS"),
            Row("carmel_system", "delta2", "",
              s"${getTableLocation("delta2")}", true, 2L) ::
              Row("carmel_system", "carmel_delta_tables", "",
                s"${getTableLocation("carmel_delta_tables")}", true, 2L) :: Nil)
        }
      }
    }
  }

  test("test delta temporary table") {
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
      "io.delta.sql.DeltaSparkSessionExtension")
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, true)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    withDatabase("carmel_system") {
      sql("create database carmel_system")
      sql("use carmel_system")
      withTempPaths(numPaths = 2) { case Seq(dir, dir1) =>
        withTable(s"$DELTA_META_TABLE_NAME", "delta1", "delta2", "t_delta") {
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
            s"""
               |CREATE TEMPORARY TABLE t_delta(id INT) USING parquet
               |""".stripMargin)
          sql(
            """
              |CONVERT TO DELTA delta1
              |""".stripMargin)
          sql(
            """
              |CONVERT TO DELTA t_delta
              |""".stripMargin)
          Thread.sleep(3000)
          checkAnswer(
            sql("SHOW DELTAS"),
            Row("carmel_system", "delta1", "",
              s"${getTableLocation("delta1")}", true, 2L) ::
              Row("carmel_system", "carmel_delta_tables", "",
                s"${getTableLocation("carmel_delta_tables")}", true, 2L) :: Nil)
          sql(
            """
              |ALTER TABLE delta1 RENAME TO delta2
              |""".stripMargin)
          sql(
            """
              |DROP TABLE t_delta
              |""".stripMargin)
          Thread.sleep(3000)
          checkAnswer(
            sql("SHOW DELTAS"),
            Row("carmel_system", "delta2", "",
              s"${getTableLocation("delta2")}", true, 2L) ::
              Row("carmel_system", "carmel_delta_tables", "",
                s"${getTableLocation("carmel_delta_tables")}", true, 2L) :: Nil)
        }
      }
    }
  }

  test("test using jdbc instead of delta table") {
    sys.props("spark.testing") = "true"
    spark.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
      "io.delta.sql.DeltaSparkSessionExtension")
    spark.sparkContext.conf.set(DeltaSQLConf.AUTO_VACUUM_ENABLED, false)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    spark.sparkContext.conf.set(DeltaSQLConf.META_TABLE_STORAGE, "jdbc")
    spark.sparkContext.conf.set("spark.viewpoint.mysql.url", "jdbc:derby:target/carmel_system")
    spark.sparkContext.conf.set("spark.viewpoint.mysql.username", "root")
    spark.sparkContext.conf.set("spark.viewpoint.mysql.password", "")
    Utils.classForName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection("jdbc:derby:target/carmel_system;create=true")
    try {
      try {
        val create = conn.createStatement
        try {
          create.execute(DELTA_META_TABLE_DROP_SQL)
        } catch {
          case e: SQLException =>
          // table not exists
        }
        create.execute(DELTA_META_TABLE_CREATION_SQL2)
        create.close()
      } catch {
        case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
      }
    } finally {
      conn.close()
    }
    // now test
    withTempPaths(numPaths = 2) { case Seq(dir1, dir2) =>
      // scalastyle:off println
      withTable("delta1", "delta2", "delta3") {
        sql(
          s"""
             |CREATE TABLE delta1(id INT) USING parquet
             |LOCATION '$dir1'
             |""".stripMargin)
        sql(
          s"""
             |CREATE TABLE delta2(id INT) USING parquet
             |LOCATION '$dir2'
             |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA delta1
            |""".stripMargin)
        Thread.sleep(7000)
        checkAnswer(
          sql("SHOW DELTAS"),
          Row("default", "delta1", "",
            s"${getTableLocation("delta1")}", true, 2L) :: Nil)
        sql(
          """
            |ALTER TABLE delta1 RENAME TO delta3
            |""".stripMargin)
        Thread.sleep(7000)
        checkAnswer(
          sql("SHOW DELTAS"),
          Row("default", "delta3", "",
            s"${getTableLocation("delta3")}", true, 2L) :: Nil)
        sql(
          """
            |CONVERT TO DELTA delta2
            |""".stripMargin)
        Thread.sleep(7000)
        checkAnswer(
          sql("SHOW DELTAS"),
          Row("default", "delta3", "",
            s"${getTableLocation("delta3")}", true, 2L) ::
            Row("default", "delta2", "",
              s"${getTableLocation("delta2")}", true, 2L) :: Nil)
        sql(
          """
            |DROP TABLE delta2
            |""".stripMargin)
        Thread.sleep(7000)
        checkAnswer(
          sql("SHOW DELTAS"),
          Row("default", "delta3", "",
            s"${getTableLocation("delta3")}", true, 2L) :: Nil)

        sql(
          """
            |CONVERT TO PARQUET delta3
            |""".stripMargin)
        Thread.sleep(7000)
        checkAnswer(sql("SHOW DELTAS"), Nil)
      }
    }
  }
}
