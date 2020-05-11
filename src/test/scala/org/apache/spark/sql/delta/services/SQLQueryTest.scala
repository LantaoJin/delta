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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class SQLQuerySuite extends QueryTest
  with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {
  import testImplicits._

  test("test convert to delta without schema infer") {
    withTable("test1") {
      sql(
        """
          |CREATE TABLE test1 (col1 INT, col2 INT) USING parquet
          |""".stripMargin)
      for(i <- 0 to 50) {
        sql(
          s"""
             |INSERT INTO TABLE test1 VALUES ($i, $i)
             |""".stripMargin)
      }

      var numJobs = 0
      val jobListener = new SparkListener() {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          numJobs += 1
        }
      }
      spark.sparkContext.addSparkListener(jobListener)
      sql("CONVERT TO DELTA test1")
      Thread.sleep(5000)
      assert(numJobs > 0)
      assert(numJobs < 6) // fail without patch CARMEL-2583
    }
  }

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
        withTempView("test", "test2", "source") {
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
      withTempView("source") {
        (1 to 3).map(x => (x, x.toString)).toDF("id", "col1").createOrReplaceTempView("source")
        sql(
          """
            |CREATE TABLE target LIKE source
            |""".stripMargin)
      }
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

  test("test time travel rollback command") {
    withSQLConf(DeltaSQLConf.RESOLVE_TIME_TRAVEL_ON_IDENTIFIER.key -> "true") {
      withTable("test") {
        withTempView("source") {
          spark.range(0, 11).toDF("col").createOrReplaceTempView("source")
          sql(
            """
              |CREATE TABLE test USING parquet AS SELECT col FROM source
              |""".stripMargin)
        }
        sql(
          """
            |CONVERT TO DELTA test
            |""".stripMargin)
        // make sure we have two checkpoint files: 0 and 10
        for (i <- 0 to 10) {
          sql(
            s"""
              |DELETE FROM test WHERE col=$i
              |""".stripMargin)
        }
        assert(sql("DESC HISTORY test").count() == 12)

        sql(
          """
            |ROLLBACK test AT version=5
            |""".stripMargin)
        assert(sql("DESC HISTORY test").count() == 6)
        checkAnswer(
          sql("SELECT * FROM test"),
          Row(5) :: Row(6) :: Row(7) :: Row(8) :: Row(9) :: Row(10) :: Nil
        )

        sql(
          """
            |ROLLBACK test AT version=0
            |""".stripMargin)
        assert(sql("DESC HISTORY test").count() == 1)
        checkAnswer(
          sql("SELECT * FROM test"),
          Row(0) :: Row(1) :: Row(2) :: Row(3) :: Row(4) :: Row(5) ::
          Row(6) :: Row(7) :: Row(8) :: Row(9) :: Row(10) :: Nil
        )

        // throws exception when rollback to a non-exists version
        val e = intercept[AnalysisException](
          sql(
            """
              |ROLLBACK test AT version=5
              |""".stripMargin)
        ).getMessage()
        assert(e.contains("Rollback to invalid version 5"))

        // we overwrite the version 5, then rollback
        for (i <- 0 to 5) {
          sql(
            s"""
               |DELETE FROM test WHERE col=$i * 2
               |""".stripMargin)
        }
        sql(
          """
            |ROLLBACK test AT version=5
            |""".stripMargin)
        assert(sql("DESC HISTORY test").count() == 6)
        checkAnswer(
          sql("SELECT * FROM test"),
          Row(1) ::  Row(3) :: Row(5) :: Row(7) :: Row(9) :: Row(10) :: Nil
        )
      }
    }
  }

  test("test time travel rollback command on partition table") {
    withTable("test1") {
      withTempView("source") {
        spark.range(0, 10).map(x => (x, (x % 2).toString)).toDF("id", "date")
          .createOrReplaceTempView("source")
        sql(
          """
            |CREATE TABLE test1(id INT, date STRING) USING parquet
            |PARTITIONED BY (date)
            |""".stripMargin)
        sql(
          """
            |INSERT INTO test1 SELECT * FROM source
            |""".stripMargin)
        checkAnswer(
          sql("SHOW PARTITIONS test1"),
          Row("date=0") :: Row("date=1") :: Nil
        )

        sql("CONVERT TO DELTA test1")

        for (i <- 10 to 22) {
          sql(s"INSERT INTO test1 VALUES ($i, ${i.toString})")
        }
        checkAnswer(
          sql("SELECT * FROM test1 WHERE date='10'"),
          Row(10, "10") :: Nil
        )

        sql("ROLLBACK test1 AT version=0")
        checkAnswer(sql("SELECT * FROM test1 WHERE date='10'"), Nil)
        checkAnswer(
          sql("SHOW PARTITIONS test1"),
          Row("date=0") :: Row("date=1") :: Nil
        )
      }
    }
  }

  test("reduce columns before join") {
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
          |INSERT INTO TABLE target VALUES (3, 3)
          |""".stripMargin)
      sql(
        """
          |CONVERT TO DELTA target
          |""".stripMargin)
      sql(
        """
          |CREATE TABLE source (a INT, b STRING, c INT, d STRING) USING parquet
          |""".stripMargin)
      sql(
        """
          |INSERT INTO TABLE source VALUES (2, "2000", 20, "hello")
          |""".stripMargin)
      sql(
        """
          |INSERT INTO TABLE source VALUES (3, "3000", 30, "world")
          |""".stripMargin)
      sql(
        s"""
           |UPDATE t
           |FROM target t, source s
           |SET t.col2 = (t.col2 + s.c)
           |WHERE t.col1 = s.a
           |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, 1) :: Row(2, 22) :: Row(3, 33) :: Nil
      )
      sql(
        s"""
           |DELETE t
           |FROM target t, source s
           |WHERE t.col1 = s.a
           |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, 1) :: Nil
      )
    }
  }

  test("resolve assignment keys when a field has same name in target and source") {
    withTable("target", "source") {
      sql(
        """
          |CREATE TABLE target (col1 INT, col2 STRING) USING parquet
          |""".stripMargin)
      sql("INSERT INTO target VALUES (1, 'A')")
      sql(
        """
          |CREATE TABLE source (col1 INT, col2 STRING) USING parquet
          |""".stripMargin)
      sql("INSERT INTO source VALUES (1, 'B')")
      sql(
        """
          |CONVERT TO DELTA target
          |""".stripMargin)
      sql( // simple update
        """
          |UPDATE target
          |SET col2 = 'a'
          |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, "a") :: Nil
      )
      sql( // simple update
        """
          |UPDATE target
          |SET target.col2 = 'b'
          |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, "b") :: Nil
      )
      sql( // simple update
        """
          |UPDATE target t
          |SET col2 = 'c'
          |WHERE col1 = 1
          |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, "c") :: Nil
      )
      sql( // simple update with alias
        """
          |UPDATE target t
          |SET t.col2 = 'd'
          |WHERE t.col1 = 1
          |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, "d") :: Nil
      )
      sql( // cross table update with "SET col2 ="
        s"""
           |UPDATE t
           |FROM target t, source s
           |SET col2 = CASE WHEN s.col2 IS NOT NULL THEN s.col2 ELSE t.col2 END
           |WHERE t.col1 = s.col1
           |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, "B") :: Nil
      )
      sql( // cross table update with "SET t.col2 ="
        s"""
           |UPDATE t
           |FROM target t, source s
           |SET t.col2 = CASE WHEN s.col2 IS NOT NULL THEN s.col2 ELSE t.col2 END
           |WHERE t.col1 = s.col1
           |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM target"),
        Row(1, "B") :: Nil
      )
    }
  }

  test("test multiple source rows match on the same target row") {
    withTable("t1", "t2") {
      sql("create table t1(a int, b string) using parquet")
      sql("create table t2(a int, b string) using parquet")
      sql("insert into table t1 select 1, 'abc'")
      sql("insert into table t1 select 2, 'ccc'")
      sql("insert into table t2 select 1, 'ab'")
      sql("insert into table t2 select 1, 'aabb'")
      sql("insert into table t2 select 1, 'aabbcc'")
      sql("insert into table t2 select 2, 'aabb'")
      sql("CONVERT TO DELTA t1")
      sql("CONVERT TO DELTA t2")
      val e = intercept[UnsupportedOperationException] {
        sql(
          """
            |UPDATE t1
            |FROM t1, t2
            |SET t1.b = t2.b
            |WHERE t1.a = t2.a
            |""".stripMargin)
      }.getMessage
      assert(e.contains("Cannot perform UPDATE as multiple source rows matched"))
      // scalastyle:off println
      println(e)
      // scalastyle:on println
      sql("DELETE FROM t2 WHERE t2.b='ab' OR t2.b='aabbcc'")
      sql(
        """
          |UPDATE t1
          |FROM t1, t2
          |SET t1.b = t2.b
          |WHERE t1.a = t2.a
          |""".stripMargin)
      checkAnswer(
        sql("select * from t1"),
        Row(1, "aabb") :: Row(2, "aabb") :: Nil)
    }
  }
}
