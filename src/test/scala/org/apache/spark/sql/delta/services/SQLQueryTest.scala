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

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.commands.{DeleteWithJoinCommand, UpdateWithJoinCommand}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.metric.SQLMetric
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
          "numRemovedFiles -> 2", "numRowsDeleted -> 2", "numAddedFiles -> 1")
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
          "numRemovedFiles -> 1", "numRowsDeleted -> 1", "numAddedFiles -> 1")
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

  def checkedMetrics(df: DataFrame): Map[String, SQLMetric] = {
    val converted = df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec =>
        a.executedPlan
      case plan => plan
    }
    converted match {
      case ExecutedCommandExec(cmd) => cmd.metrics
      case _ => converted.metrics
    }
  }

  test("test clean up staging file when job fail - partition table") {
    Seq(true, false).foreach { ae =>
      withTempDir { dir =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
          withTable("test1") {
            withTempView("source") {
              spark.range(0, 4).map(x => (x, (x % 2).toString)).toDF("id", "date")
                .createOrReplaceTempView("source")
              sql(
                s"""
                   |CREATE TABLE test1(id INT, date STRING) USING parquet
                   |PARTITIONED BY (date)
                   |LOCATION '$dir'
                   |""".stripMargin)
              sql(
                """
                  |INSERT INTO test1 SELECT * FROM source
                  |""".stripMargin)
              sql("CONVERT TO DELTA test1")

              val failingUdf = org.apache.spark.sql.functions.udf {
                (id: Long) => {
                  throw new RuntimeException("testing error")
                }
              }
              spark.udf.register("fail", failingUdf)
              val e = intercept[SparkException] {
                sql(
                  """
                    |UPDATE test1
                    |SET id = fail(id)
                    |""".stripMargin)
              }.getMessage
              assert(e.contains("Job aborted"))
              // this maybe failed due to job aborted but still has new task submitting
              dir.listFiles().foreach { f =>
                if (f.isDirectory && f.getName.startsWith(".spark-staging")) {
                  assert(f.list().isEmpty)
                }
              }
              val df1 = sql(
                """
                  |UPDATE test1
                  |SET date = "0"
                  |WHERE id % 2 = 0
                  |""".stripMargin)
              checkAnswer(
                sql("SELECT count(*) FROM test1 WHERE date = '0'"),
                Row(2) :: Nil
              )
              assert(checkedMetrics(df1)("numRowsUpdated").value == 2)
              dir.listFiles().foreach { f =>
                if (f.isDirectory && f.getName.startsWith(".spark-staging")) {
                  assert(f.list().isEmpty)
                }
              }
              val df2 = sql(
                """
                  |DELETE FROM test1
                  |WHERE id % 2 = 0
                  |""".stripMargin)
              checkAnswer(
                sql("SELECT count(*) FROM test1 WHERE date != '0'"),
                Row(2) :: Nil
              )
              assert(checkedMetrics(df2)("numRowsDeleted").value == 2)
              dir.listFiles().foreach { f =>
                if (f.isDirectory && f.getName.startsWith(".spark-staging")) {
                  assert(f.list().isEmpty)
                }
              }
            }
          }
        }
      }
    }
  }

  test("test clean up staging file when job fail - bucket table") {
    Seq(true, false).foreach { ae =>
      withTempDir { dir =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
          withTable("test1") {
            withTempView("source") {
              spark.range(0, 4).map(x => (x, (x % 2).toString)).toDF("id", "date")
                .createOrReplaceTempView("source")
              sql(
                s"""
                   |CREATE TABLE test1(id INT, date STRING) USING parquet
                   |CLUSTERED BY (date)
                   |INTO 2 BUCKETS
                   |LOCATION '$dir'
                   |""".stripMargin)
              sql(
                """
                  |INSERT INTO test1 SELECT * FROM source
                  |""".stripMargin)
              sql("CONVERT TO DELTA test1")

              val failingUdf = org.apache.spark.sql.functions.udf {
                (id: Long) => {
                  throw new RuntimeException("testing error")
                }
              }
              spark.udf.register("fail", failingUdf)
              val e = intercept[SparkException] {
                sql(
                  """
                    |UPDATE test1
                    |SET id = fail(id)
                    |""".stripMargin)
              }.getMessage
              assert(e.contains("Job aborted"))
              // this maybe failed due to job aborted but still has new task submitting
              dir.listFiles().foreach { f =>
                if (f.isDirectory && f.getName.startsWith(".spark-staging")) {
                  assert(f.list().isEmpty)
                }
              }
              val df1 = sql(
                """
                  |UPDATE test1
                  |SET date = "t"
                  |WHERE id % 2 = 0
                  |""".stripMargin)
              checkAnswer(
                sql("SELECT count(*) FROM test1 WHERE date = 't'"),
                Row(2) :: Nil
              )
              assert(checkedMetrics(df1)("numRowsUpdated").value == 2)
              dir.listFiles().foreach { f =>
                if (f.isDirectory && f.getName.startsWith(".spark-staging")) {
                  assert(f.list().isEmpty)
                }
              }
              val df2 = sql(
                """
                  |DELETE FROM test1
                  |WHERE id % 2 = 0
                  |""".stripMargin)
              checkAnswer(
                sql("SELECT count(*) FROM test1 WHERE date != 't'"),
                Row(2) :: Nil
              )
              assert(checkedMetrics(df2)("numRowsDeleted").value == 2)
              dir.listFiles().foreach { f =>
                if (f.isDirectory && f.getName.startsWith(".spark-staging")) {
                  assert(f.list().isEmpty)
                }
              }
            }
          }
        }
      }
    }
  }

  test("test") {
    withTable("delta_bucket", "test_table") {
      withTempView("test") {
        spark.range(50000).map(x => (x, x + 1, x.toString)).toDF("id", "num", "name")
          .createOrReplaceTempView("test")
        sql(
          """
            |CREATE TABLE test_table USING parquet
            |AS SELECT * FROM test
            |""".stripMargin)
        sql(
          s"""
             |CREATE TABLE delta_bucket USING parquet
             |AS SELECT * FROM test
             |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA delta_bucket
            |""".stripMargin)
        sql(
          """
            |UPDATE t
            |FROM delta_bucket t, test_table s
            |SET t.num = 0
            |WHERE t.id = s.id AND s.id % 2 = 0
            |""".stripMargin)
        checkAnswer(
          sql("SELECT count(*) FROM delta_bucket WHERE num = 0"),
          Row(25000) :: Nil
        )
      }
    }
  }

  def getConditions(df: DataFrame): Seq[Expression] = {
    val conds = df.queryExecution.optimizedPlan match {
      case u @ UpdateWithJoinCommand(_, _, _, Some(cond), _) =>
        splitConjunctivePredicates(cond)
      case d @ DeleteWithJoinCommand(_, _, _, Some(cond), _) =>
        splitConjunctivePredicates(cond)
      case _ => Nil
    }
    // scalastyle:off println
    conds.map { c => println(c.simpleString); c }
    // scalastyle:on println
  }

  test("test resolution") {
    withTable("target", "source") {
      withTempView("test") {
        spark.range(5).map(x => (x, x + 1, x.toString)).toDF("id", "num", "name")
          .createOrReplaceTempView("test")
        sql(
          """
            |CREATE TABLE source USING parquet
            |AS SELECT * FROM test
            |""".stripMargin)
        sql(
          s"""
             |CREATE TABLE target USING parquet
             |AS SELECT * FROM test
             |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA target
            |""".stripMargin)

        val df = sql(
          """
            |UPDATE t
            |FROM target t, source s
            |SET t.num = 0
            |WHERE t.id = s.id AND s.id % 2 = 0
            |""".stripMargin)
        assert(getConditions(df).map(_.canonicalizedIgnoreExprId.simpleString).sorted
          === "((id#0L % 2) = 0)" :: "((id#0L % 2) = 0)" :: "(id#0L = id#0L)" :: Nil)

        val df2 = sql(
          """
            |DELETE t
            |FROM target t, source s
            |WHERE t.id = s.id
            |""".stripMargin)
        assert(getConditions(df2).map(_.canonicalizedIgnoreExprId.simpleString).sorted
          === "(id#0L = id#0L)" :: Nil)

        val df3 = sql(
          """
            |UPDATE t
            |FROM target t, source s
            |SET t.num = 0
            |WHERE t.id = s.id AND t.num = s.num
            |AND t.id > 5 AND s.num = 10
            |""".stripMargin)
        assert(getConditions(df3).map(_.canonicalizedIgnoreExprId.simpleString).sorted
          === "(id#0L = id#0L)" :: "(id#0L > 5)" :: "(id#0L > 5)" :: "(num#0L = 10)" ::
          "(num#0L = 10)" :: "(num#0L = num#0L)" :: Nil)
      }
    }
  }

  test("repartition to merge small files on update/delete for partition delta table") {
    Seq(true, false).foreach { ae =>
      val shufflePartitionNum = 5
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
        SQLConf.AUTO_REPARTITION_FOR_WRITING_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.REDUCE_POST_SHUFFLE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> s"$shufflePartitionNum",
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
        withTable("tt1", "tt2") {
          spark.range(0, 50).map(x => (x.toString, x, x % 10)).toDF("a", "b", "c")
            .write.saveAsTable("tt1")

          sql(
            s"""
               |CREATE TABLE tt2(a int, b int, dt1 int) USING PARQUET
               |PARTITIONED BY (dt1)
               |""".stripMargin)

          sql("convert to delta tt2")

          val df = sql(s"INSERT INTO tt2 SELECT a, b, c from tt1")
          assert(checkedMetrics(df)("numFiles").value == 10)

          val df2 = sql(
            """
              |UPDATE t
              |FROM tt2 t, tt1 s
              |SET t.b = s.b
              |WHERE t.a = s.a
              |""".stripMargin)
          assert(checkedMetrics(df2)("numAddedFiles").value == 10)

          val df3 = sql(
            """
              |DELETE t
              |FROM tt2 t, tt1 s
              |WHERE t.a = s.a AND s.a % 3 = 0
              |""".stripMargin)
          sql("desc history tt2").show(false)
          assert(checkedMetrics(df3)("numAddedFiles").value == 10)
        }
      }
    }
  }

  test("repartition to merge small files on update/delete for non-partition delta table") {
    Seq(true, false).foreach { ae =>
      val shufflePartitionNum = 5
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
        SQLConf.AUTO_REPARTITION_FOR_WRITING_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.REDUCE_POST_SHUFFLE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> s"$shufflePartitionNum",
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
        withTable("tt1", "tt2") {
          spark.range(0, 50).map(x => (x.toString, x)).toDF("a", "b")
            .write.saveAsTable("tt1")

          sql(
            s"""
               |CREATE TABLE tt2(a int, b int) USING PARQUET
               |""".stripMargin)

          sql("convert to delta tt2")

          val df = sql(s"INSERT INTO tt2 SELECT a, b from tt1")
          assert(checkedMetrics(df)("numFiles").value == 5)

          val df2 = sql(
            """
              |UPDATE t
              |FROM tt2 t, tt1 s
              |SET t.b = s.b
              |WHERE t.a = s.a
              |""".stripMargin)
          assert(checkedMetrics(df2)("numRowsUpdated").value == 50)
          assert(checkedMetrics(df2)("numAddedFiles").value == 5)

          val df3 = sql(
            """
              |DELETE t
              |FROM tt2 t, tt1 s
              |WHERE t.a = s.a AND s.a % 3 = 0
              |""".stripMargin)
          sql("desc history tt2").show(false)
          assert(checkedMetrics(df3)("numAddedFiles").value == 5)
        }
      }
    }
  }


  test("repartition to merge small files on update/delete for partition-bucket delta table") {
    Seq(true, false).foreach { ae =>
      val shufflePartitionNum = 5
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
        SQLConf.AUTO_REPARTITION_FOR_WRITING_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.REDUCE_POST_SHUFFLE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> s"$shufflePartitionNum",
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
        withTable("tt1", "tt2") {
          spark.range(0, 50).map(x => (x.toString, x, x, x % 10)).toDF("a", "b", "c", "d")
            .write.saveAsTable("tt1")

          sql(
            s"""
               |CREATE TABLE tt2(a int, b int, c int, dt1 int) USING PARQUET
               |CLUSTERED BY (c)
               |INTO 2 BUCKETS
               |PARTITIONED BY (dt1)
               |""".stripMargin)

          sql("convert to delta tt2")

          val df = sql(s"INSERT INTO tt2 SELECT a, b, c, d from tt1")
          assert(checkedMetrics(df)("numFiles").value == 20)

          val df2 = sql(
            """
              |UPDATE t
              |FROM tt2 t, tt1 s
              |SET t.c = s.c + 100
              |WHERE t.a = s.a
              |""".stripMargin)
          assert(checkedMetrics(df2)("numAddedFiles").value == 20)

          val df3 = sql(
            """
              |DELETE t
              |FROM tt2 t, tt1 s
              |WHERE t.a = s.a AND s.a % 3 = 0
              |""".stripMargin)
          sql("desc history tt2").show(false)
          assert(checkedMetrics(df3)("numAddedFiles").value == 13)
        }
      }
    }
  }

  test("repartition to merge small files on update/delete for non-partition-bucket delta table") {
    Seq(true, false).foreach { ae =>
      val shufflePartitionNum = 5
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
        SQLConf.AUTO_REPARTITION_FOR_WRITING_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.REDUCE_POST_SHUFFLE_PARTITIONS_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> s"$shufflePartitionNum",
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
        withTable("tt1", "tt2") {
          spark.range(0, 50).map(x => (x.toString, x, x)).toDF("a", "b", "c")
            .write.saveAsTable("tt1")

          sql(
            s"""
               |CREATE TABLE tt2(a int, b int, c int) USING PARQUET
               |CLUSTERED BY (c)
               |INTO 2 BUCKETS
               |""".stripMargin)

          sql("convert to delta tt2")

          val df = sql(s"INSERT INTO tt2 SELECT a, b, c from tt1")
          assert(checkedMetrics(df)("numFiles").value == 2)

          val df2 = sql(
            """
              |UPDATE t
              |FROM tt2 t, tt1 s
              |SET t.c = s.c + 100
              |WHERE t.a = s.a
              |""".stripMargin)
          assert(checkedMetrics(df2)("numAddedFiles").value == 2)

          val df3 = sql(
            """
              |DELETE t
              |FROM tt2 t, tt1 s
              |WHERE t.a = s.a AND s.a % 3 = 0
              |""".stripMargin)
          sql("desc history tt2").show(false)
          assert(checkedMetrics(df3)("numAddedFiles").value == 2)
        }
      }
    }
  }

  test("repartition to merge small files on update/delete" +
    " on partition delta table join with bucket table") {
    Seq(true, false).foreach { ae =>
      val shufflePartitionNum = 5
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> ae.toString,
        SQLConf.AUTO_REPARTITION_FOR_WRITING_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0",
        SQLConf.SHUFFLE_PARTITIONS.key -> s"$shufflePartitionNum",
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
        withTable("t1", "t2", "tt1", "tt2") {
          spark.range(0, 50).map(x => (x, x, x)).toDF("a", "b", "c")
            .write.saveAsTable("t1")
          spark.range(0, 50).map(x => (x, x, (x % 10))).toDF("a", "b", "c")
            .write.saveAsTable("t2")

          sql(
            s"""
               |CREATE TABLE tt1(id int, col int, dt int) USING PARQUET
               |CLUSTERED BY (id)
               |INTO 10 BUCKETS
               |""".stripMargin)

          sql(
            s"""
               |CREATE TABLE tt2(id int, col int, dt int) USING PARQUET
               |PARTITIONED BY (dt)
               |""".stripMargin)

          sql("convert to delta tt1")
          sql("convert to delta tt2")

          val df1 = sql(s"INSERT INTO tt1 SELECT a, b, c from t1")
          assert(checkedMetrics(df1)("numFiles").value == 10)

          val df2 = sql(s"INSERT INTO tt2 SELECT a, b, c from t2")
          assert(checkedMetrics(df2)("numFiles").value == 10)

          val df3 = sql(
            """
              |UPDATE t
              |FROM tt2 t, tt1 s
              |SET t.dt = s.dt, t.col = s.col
              |WHERE t.id = s.id AND t.dt > 5
              |""".stripMargin)
          assert(checkedMetrics(df3)("numAddedFiles").value == 20)
        }
      }
    }
  }
}
