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

import java.io.File
import java.sql.{DriverManager, SQLException}

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaTestSparkSession}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.util.Utils

class AutoVacuumSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {

  private var tempDir: File = _

  override def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new DeltaTestSparkSession(sparkConf)
    session.sparkContext.conf.set(config.AUTO_VACUUM_ENABLED, true)
    session.sparkContext.conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS,
      Seq(classOf[DeltaSparkSessionExtension].getName))
    session.sparkContext.conf.set(config.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    session.sparkContext.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[DeltaCatalog].getName)
    session.sparkContext.conf.set(config.META_TABLE_IDENTIFIER, DELTA_META_TABLE_NAME)
    session.sparkContext.conf.set(config.META_TABLE_STORAGE, "jdbc")
    tempDir = Utils.createTempDir()
//    tempDir.delete()
    session.sparkContext.conf.set(config.WORKSPACE_BASE_DIR, tempDir.getCanonicalPath)
    session.sparkContext.conf.set("spark.viewpoint.mysql.url", "jdbc:derby:target/carmel_system")
    session.sparkContext.conf.set("spark.viewpoint.mysql.username", "root")
    session.sparkContext.conf.set("spark.viewpoint.mysql.password", "")
    session
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    sys.props("spark.testing") = "true"
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
  }

  override def afterEach(): Unit = {
    super.afterEach()
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
        create.close()
      } catch {
        case e: SQLException if e.getSQLState == "X0Y32" =>
        // table exists
      }
    } finally {
      conn.close()
    }
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

  test("test rename and drop delta for metadata table") {
    withTempPaths(numPaths = 2) { case Seq(dir, dir1) =>
      withTable("delta1", "delta2") {
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
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta1", "",
            s"${getTableLocation("delta1")}", true, 2L) :: Nil)
        sql(
          """
            |ALTER TABLE delta1 RENAME TO delta2
            |""".stripMargin)
        Thread.sleep(3000)
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta2", "",
            s"${getTableLocation("delta2")}", true, 2L) :: Nil)
      }
    }
  }

  test("test delta temporary table") {
    withTempPaths(numPaths = 2) { case Seq(dir, dir1) =>
      withTable("delta1", "delta2", "t_delta") {
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
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta1", "",
            s"${getTableLocation("delta1")}", true, 2L) :: Nil)
        sql(
          """
            |ALTER TABLE delta1 RENAME TO delta2
            |""".stripMargin)
        sql(
          """
            |DROP TABLE t_delta
            |""".stripMargin)
        Thread.sleep(3000)
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta2", "",
            s"${getTableLocation("delta2")}", true, 2L) :: Nil)
      }
    }
  }

  test("test using jdbc instead of delta table") {
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
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta1", "",
            s"${getTableLocation("delta1")}", true, 2L) :: Nil)
        sql(
          """
            |ALTER TABLE delta1 RENAME TO delta3
            |""".stripMargin)
        Thread.sleep(7000)
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta3", "",
            s"${getTableLocation("delta3")}", true, 2L) :: Nil)
        sql(
          """
            |CONVERT TO DELTA delta2
            |""".stripMargin)
        Thread.sleep(7000)
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta3", "",
            s"${getTableLocation("delta3")}", true, 2L) ::
            Row("default", "delta2", "",
              s"${getTableLocation("delta2")}", true, 2L) :: Nil)
        sql(
          """
            |DROP TABLE delta2
            |""".stripMargin)
        Thread.sleep(7000)
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("default", "delta3", "",
            s"${getTableLocation("delta3")}", true, 2L) :: Nil)

        sql(
          """
            |CONVERT TO PARQUET delta3
            |""".stripMargin)
        Thread.sleep(7000)
        assert(DeltaTableMetadata.getRowsFromMetadataTable(spark).isEmpty)
      }
    }
  }

  test("test double check delta table") {
      val db1 = new File(tempDir, "P_abc_t")
      db1.mkdir()
      val db2 = new File(tempDir, "P_def_T")
      db2.mkdir()
      val invalid = new File(tempDir, "d_ghi_t")
      invalid.mkdir()
      val file = new File(tempDir, "p_file_t")
      file.createNewFile()
    withDatabase("P_ABC_T", "P_DEF_T") {
      sql("create database P_ABC_T")
      sql("create database P_DEF_T")
      withTable("P_ABC_T.t1", "P_ABC_T.t2", "P_DEF_T.t1") {
        sql("create table P_ABC_T.t1 (id int) using delta")
        sql("create table P_ABC_T.t2 (id int) using delta")
        sql("create table P_DEF_T.t1 (id int) using delta")
        Thread.sleep(15000)
        assert(
          DeltaTableMetadata.getRowsFromMetadataTable(spark) ===
          Row("p_abc_t", "t1", "",
            s"${getTableLocation("t1", Some("p_abc_t"))}", true, 2L) ::
          Row("p_abc_t", "t2", "",
            s"${getTableLocation("t2", Some("p_abc_t"))}", true, 2L) ::
          Row("p_def_t", "t1", "",
            s"${getTableLocation("t1", Some("p_def_t"))}", true, 2L) :: Nil)
      }
    }
  }
}
