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

package org.apache.spark.sql.delta

import java.io.{File, FileNotFoundException}

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.util.Utils

class DeltaSQLQuerySuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest with SQLTestUtils {
  import testImplicits._

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
            |ROLLBACK test AT 5
            |""".stripMargin)
        assert(sql("DESC HISTORY test").count() == 6)
        checkAnswer(
          sql("SELECT * FROM test"),
          Row(5) :: Row(6) :: Row(7) :: Row(8) :: Row(9) :: Row(10) :: Nil
        )

        sql(
          """
            |ROLLBACK test AT 0
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
              |ROLLBACK test AT 5
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
            |ROLLBACK test AT 5
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

        sql("CONVERT TO DELTA test1 PARTITIONED BY (date STRING)")

        for (i <- 10 to 22) {
          sql(s"INSERT INTO test1 VALUES ($i, ${i.toString})")
        }
        checkAnswer(
          sql("SELECT * FROM test1 WHERE date='10'"),
          Row(10, "10") :: Nil
        )

        sql("ROLLBACK test1 AT 0")
        checkAnswer(sql("SELECT * FROM test1 WHERE date='10'"), Nil)
//        checkAnswer(
//          sql("SHOW PARTITIONS test1"),
//          Row("date=0") :: Row("date=1") :: Nil
//        )
      }
    }
  }

  private def catalog: TableCatalog = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[DeltaCatalog]
  }

  private def checkPartitions(name: String, partitionNames: String*) = {
    val table = catalog.loadTable(Identifier.of(Array("default"), name))
    assert(table.partitioning === partitionNames.map(p => IdentityTransform(FieldReference(p))))
  }

  test("test convert a partitioned table") {
    withTable("part1", "part2", "part3", "part4") {
      withSQLConf(DeltaSQLConf.USE_SCHEMA_FROM_EXISTS_TABLE.key -> "false") {
        sql(
          """
            |CREATE TABLE part1 (id int, col1 string)
            |USING parquet
            |PARTITIONED BY (col1)
            |""".stripMargin)
        val e1 = intercept[FileNotFoundException] {
          sql(
            """
              |CONVERT TO DELTA part1
              |""".stripMargin)
        }.getMessage
        assert(e1.contains("No file found in the directory"))
        sql(
          """
            |INSERT INTO part1 VALUES (1, "1")
            |""".stripMargin)
        val e2 = intercept[AnalysisException] {
          sql(
            """
              |CONVERT TO DELTA part1
              |""".stripMargin)
        }.getMessage
        assert(e2.contains("Expecting 0 partition column(s)"))
        sql(
          """
            |CONVERT TO DELTA part1 PARTITIONED BY (col1 STRING)
            |""".stripMargin)
        checkPartitions("part1", "col1")
      }
      withSQLConf(DeltaSQLConf.USE_SCHEMA_FROM_EXISTS_TABLE.key -> "true") {
        sql(
          """
            |CREATE TABLE part2 (id int, col1 string)
            |USING parquet
            |PARTITIONED BY (col1)
            |""".stripMargin)
        sql(
          """
            |INSERT INTO part2 VALUES (1, "1")
            |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA part2
            |""".stripMargin)
        checkPartitions("part2", "col1")

        sql(
          """
            |CREATE TABLE part3 (id int, col1 string, col2 string)
            |USING parquet
            |PARTITIONED BY (col1, col2)
            |""".stripMargin)
        sql(
          """
            |INSERT INTO part3 VALUES (1, "1", "a")
            |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA part3
            |""".stripMargin)
        checkPartitions("part3", "col1", "col2")

        sql(
          """
            |CREATE TABLE part4 (id int, col1 string)
            |USING parquet
            |PARTITIONED BY (col1)
            |""".stripMargin)
        sql(
          """
            |CONVERT TO DELTA part4
            |""".stripMargin)
        checkPartitions("part4", "col1")
      }
    }
  }

  test("temporary view may break the rule of full scan") {
    withTable("delta_table") {
      sql(
        s"""
           |CREATE TABLE delta_table
           |USING delta AS SELECT 1 AS key, 1 AS value
           """.stripMargin)

      sql("create temporary view temp_view as select key from delta_table")
      val e1 = intercept[AnalysisException] (
        sql("UPDATE temp_view v SET key=2")
      ).getMessage
      assert(e1.contains("Expect a full scan of Delta sources, but found a partial scan."))
    }
  }

  test("CTAS a delta table with the existing non-empty directory") {
    withTable("tab1") {
      val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
      try {
        // create an empty hidden file
        tableLoc.mkdir()
        val hiddenGarbageFile = new File(tableLoc.getCanonicalPath, ".garbage")
        hiddenGarbageFile.createNewFile()
        sql(s"CREATE TABLE tab1 USING DELTA AS SELECT 1, 'a'")
        checkAnswer(spark.table("tab1"), Row(1, "a"))
      } finally {
        waitForTasksToFinish()
        Utils.deleteRecursively(tableLoc)
      }
    }
  }

  test("create a managed table with the existing non-empty directory") {
    withTable("tab1") {
      val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
      try {
        // create an empty hidden file
        tableLoc.mkdir()
        val hiddenGarbageFile = new File(tableLoc.getCanonicalPath, ".garbage")
        hiddenGarbageFile.createNewFile()
        sql(s"CREATE TABLE tab1 (col1 int, col2 string) USING DELTA")
        sql("INSERT INTO tab1 VALUES (1, 'a')")
        checkAnswer(spark.table("tab1"), Row(1, "a"))
      } finally {
        waitForTasksToFinish()
        Utils.deleteRecursively(tableLoc)
      }
    }
  }
}
