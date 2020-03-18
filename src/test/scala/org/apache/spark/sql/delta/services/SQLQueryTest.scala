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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class SQLQuerySuite extends QueryTest
  with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {
  import testImplicits._

  test("test statistics") {
    withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      withTable("target") {
        sql(
          """
            |CREATE TABLE target USING parquet AS SELECT 1 as a
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE target VALUES (2)
            |""".stripMargin)
        sql(
          """
            |desc formatted target
            |""".stripMargin).show(false)
        sql(
          """
            |CONVERT TO DELTA target
            |""".stripMargin)
        val catalog = spark.sessionState.catalog
        var table = catalog.getTableMetadata(TableIdentifier("target"))
        assert(table.stats.isDefined)
        val size1 = table.stats.get.sizeInBytes

        sql(
          """
            |INSERT INTO TABLE target values (3)
            |""".stripMargin)
        sql(
          """
            |desc formatted target
            |""".stripMargin).show(false)
        table = catalog.getTableMetadata(TableIdentifier("target"))
        assert(table.stats.isDefined)
        val size2 = table.stats.get.sizeInBytes
        assert(size1 < size2)

        sql(
          """
            |UPDATE target SET a = 30 WHERE a = 3
            |""".stripMargin)
        sql(
          """
            |desc formatted target
            |""".stripMargin).show(false)
        table = catalog.getTableMetadata(TableIdentifier("target"))
        assert(table.stats.isDefined)
        val size3 = table.stats.get.sizeInBytes
        assert(size2 < size3)

        sql(
          """
            |DELETE FROM target WHERE a = 30
            |""".stripMargin)
        sql(
          """
            |desc formatted target
            |""".stripMargin).show(false)
        table = catalog.getTableMetadata(TableIdentifier("target"))
        assert(table.stats.isDefined)
        val size4 = table.stats.get.sizeInBytes
        assert(size3 < size4)

        sql(
          """
            |VACUUM target RETAIN 0 HOURS
            |""".stripMargin)
        sql(
          """
            |desc formatted target
            |""".stripMargin).show(false)
        table = catalog.getTableMetadata(TableIdentifier("target"))
        assert(table.stats.isDefined)
        val size5 = table.stats.get.sizeInBytes
        assert(size4 > size5)
      }
    }
  }

  test("test desc history and metrics") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTable("target", "source") {
        sql(
          """
            |CREATE TABLE target (col1 INT, col2 INT) USING parquet
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE target VALUES (1, 1)
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE target VALUES (2, 2)
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE target VALUES (1, 3)
            |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA target
            |""".stripMargin)

        sql(
          """
            |UPDATE target
            |SET target.col2 = 1
            |WHERE target.col1 = 1
            |""".stripMargin)
        checkAnswer(
          sql("SELECT * FROM target"),
          Row(1, 1) :: Row(1, 1) :: Row(2, 2) :: Nil)
        checkKeywordsExist(
          sql("desc history target").filter("version = '1'").select("operationMetrics"),
          "numRemovedFiles -> 2", "numRowsUpdated -> 2", "numAddedFiles -> 2")
        sql(
          """
            |DELETE FROM target
            |WHERE target.col1 = 1
            |""".stripMargin)
        checkAnswer(
          sql("SELECT * FROM target"),
          Row(2, 2) :: Nil)
        checkKeywordsExist(
          sql("desc history target").filter("version = '2'").select("operationMetrics"),
          "numRemovedFiles -> 3", "numRowsDeleted -> 2", "numAddedFiles -> 1")
        val df1 = sql(
          """
            |INSERT INTO TABLE target VALUES (1, 1)
            |""".stripMargin)
        assert(df1.queryExecution.sparkPlan.metrics("numOutputRows").value == 1)
        sql(
          """
            |CREATE TABLE source (a INT, b STRING) USING parquet
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE source VALUES (2, "2000")
            |""".stripMargin)
        sql(
          """
            |INSERT INTO TABLE source VALUES (3, "3000")
            |""".stripMargin)
        sql(
          s"""
             |UPDATE t
             |FROM target t, source s
             |SET t.col2 = (t.col2 + s.b)
             |WHERE t.col1 = s.a
             |""".stripMargin)
        checkAnswer(
          sql("SELECT * FROM target"),
          Row(1, 1) :: Row(2, 2002) :: Nil)
        checkKeywordsExist(
          sql("desc history target").filter("version = '4'").select("operationMetrics"),
          "numRemovedFiles -> 1", "numRowsUpdated -> 1", "numAddedFiles -> 1", "numSourceRows -> 2")
        val df2 = sql(
          """
            |INSERT INTO TABLE target VALUES (3, 3)
            |""".stripMargin)
        assert(df2.queryExecution.sparkPlan.metrics("numOutputRows").value == 1)
        sql(
          """
            |DELETE t
            |FROM target t, source s
            |WHERE t.col1 = s.a
            |""".stripMargin)
        checkAnswer(
          sql("SELECT * FROM target"),
          Row(1, 1) :: Nil)
        checkKeywordsExist(
          sql("desc history target").filter("version = '6'").select("operationMetrics"),
          "numRemovedFiles -> 2", "numRowsDeleted -> 2", "numAddedFiles -> 1", "numSourceRows -> 2")
        sql(
          """
            |DELETE FROM target t
            |WHERE t.col1 = 1
            |""".stripMargin)
        checkAnswer(
          sql("select * from target"), Nil)
        checkKeywordsExist(
          sql("desc history target").filter("version = '7'").select("operationMetrics"),
          "numRemovedFiles -> 3", "numRowsDeleted -> 1", "numAddedFiles -> 1")
      }
    }
  }

  test("test insert/update/delete metrics") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTable("target") {
        spark.range(5000).toDF("col").createOrReplaceTempView("test")
        spark.range(1).toDF("col").createOrReplaceTempView("source")
        sql(
          """
            |CREATE TABLE target USING parquet AS SELECT * FROM test
            |""".stripMargin)
        sql("CONVERT TO DELTA target")
        sql(
          """
            |UPDATE target
            |SET col = 0
            |WHERE col % 10 = 0
            |""".stripMargin)
        checkKeywordsExist(
          sql("desc history target").filter("version = '1'").select("operationMetrics"),
          "numRemovedFiles -> 2", "numRowsUpdated -> 500", "numAddedFiles -> 2")
        sql(
          """
            |DELETE t
            |FROM target t, source s
            |WHERE t.col = s.col
            |""".stripMargin)
        checkKeywordsExist(
          sql("desc history target").filter("version = '2'").select("operationMetrics"),
          "numRemovedFiles -> 2", "numRowsDeleted -> 500",
          "numAddedFiles -> 2", "numSourceRows -> 1")
        spark.range(5000, 8000).toDF("col").createOrReplaceTempView("test2")
        val insertDf = sql(
          """
            |INSERT INTO target SELECT * FROM test2
            |""".stripMargin)
        assert(insertDf.queryExecution.sparkPlan.metrics("numOutputRows").value == 3000)
        checkKeywordsExist(
            sql("desc history target").filter("version = '3'").select("operationMetrics"),
        "numFiles -> 2", "numOutputRows -> 3000")
        sql(
          """
            |UPDATE t
            |FROM target t, test2 s
            |SET t.col = 0
            |WHERE t.col = s.col
            |""".stripMargin)
        checkKeywordsExist(
          sql("desc history target").filter("version = '4'").select("operationMetrics"),
          "numRemovedFiles -> 2", "numRowsUpdated -> 3000",
          "numAddedFiles -> 2", "numSourceRows -> 3000")
        sql(
          """
            |UPDATE target
            |SET col = 0
            |""".stripMargin)
        checkKeywordsExist(
          sql("desc history target").filter("version = '5'").select("operationMetrics"),
          "numRemovedFiles -> 4", "numRowsUpdated -> 7500", "numAddedFiles -> 2")
        sql(
          """
            |DELETE FROM target
            |""".stripMargin)
        checkKeywordsExist(
          sql("desc history target").filter("version = '6'").select("operationMetrics"),
          "numRemovedFiles -> 2", "numRowsDeleted -> 7500", "numAddedFiles -> 0")
      }
    }
  }

  test("convert an empty table to delta") {
    def verifyUpdateDelete(tableName: String): Unit = {
      sql(
        s"""
          |DESC $tableName
          |""".stripMargin).show
      sql(
        s"""
          |INSERT INTO $tableName VALUES (4, "4")
          |""".stripMargin)
      sql(
        s"""
          |UPDATE $tableName SET col1 = "0" WHERE id % 2 = 0
          |""".stripMargin)
      sql(
        s"""
          |DELETE FROM $tableName WHERE id % 2 = 0
          |""".stripMargin)
      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
    }
    withTable("target", "empty", "part") {
      (1 to 3).map(x => (x, x.toString)).toDF("id", "col1").createOrReplaceTempView("source")
      sql(
        """
          |CREATE TABLE target LIKE source
          |""".stripMargin)
      sql(
        """
          |CONVERT TO DELTA target
          |""".stripMargin)
      verifyUpdateDelete("target")
      sql(
        """
          |CREATE TABLE empty (id int, col1 string)
          |USING parquet
          |""".stripMargin)
      sql(
        """
          |CONVERT TO DELTA empty
          |""".stripMargin)
      verifyUpdateDelete("empty")
      sql(
        """
          |CREATE TABLE part (id int, col1 string)
          |USING parquet
          |PARTITIONED BY (col1)
          |""".stripMargin)
      sql(
        """
          |CONVERT TO DELTA part
          |""".stripMargin)
      verifyUpdateDelete("part")
    }
  }
}
