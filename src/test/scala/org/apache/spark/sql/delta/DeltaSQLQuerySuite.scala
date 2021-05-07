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
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.sources.{DeltaSQLConf, DeltaSourceUtils}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.internal.SQLConf
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

  test("Apply projection pushdown to the inner join to avoid project all columns") {
    def checkProjectionPushdown(physicalPlanDescription: String): Boolean = {
      physicalPlanDescription.contains(
        "ReadSchema: struct<a:int,c:int>"
      )
    }

    var isPushdown = false
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: SparkListenerSQLExecutionStart
          if e.physicalPlanDescription.contains("CollectLimit") =>
          if (!isPushdown) { // apply once
            isPushdown = checkProjectionPushdown(e.physicalPlanDescription)
          }
        case _ => // Ignore
      }
    }

    withTable("target", "source") {
      sql("CREATE TABLE target(a int, b int, c int, d int) USING delta")
      sql("INSERT INTO TABLE target VALUES (1, 2, 3, 4)")
      sql("INSERT INTO TABLE target VALUES (2, 3, 4, 5)")
      sql("CREATE TABLE source(a int, b int, c int, d int) USING parquet")
      sql("INSERT INTO TABLE source VALUES (1, 2, 3, 5)")
      sql("INSERT INTO TABLE source VALUES (1, 1, 1, 1)")
      spark.sparkContext.addSparkListener(listener)
      sql(
        """
          |UPDATE t
          |FROM target t, source s
          |SET t.b = s.b
          |WHERE t.a = s.a and s.d = 1 and t.c = 3
          |""".stripMargin)
      spark.sparkContext.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(10))
      spark.sparkContext.removeSparkListener(listener)
      checkAnswer(spark.table("target"), Row(1, 1, 3, 4) :: Row(2, 3, 4, 5) :: Nil)
      Thread.sleep(50000)
      assert(isPushdown)
    }
  }

  test("update/delete condition should be case insensitive") {
    withTable("target", "source") {
      sql("create table source (a int , b string) using parquet")
      sql("create table target (A int , B string) using delta")
      sql("INSERT INTO target VALUES (1, 'test')")
      // below statements don't throw exception.
      sql("UPDATE t FROM target t, source s SET t.b = 'test' WHERE t.a = s.a")
      sql("DELETE t FROM target t, source s WHERE t.a = s.a")
    }
  }

  test("create temp table with using delta should set location in tmp directory") {
    withTable("temp_delta1", "temp_delta2", "temp_delta3") {
      sql(
        """
          |CREATE TEMPORARY TABLE temp_delta1 (id int, name string) using parquet
          |""".stripMargin)
      sql("CONVERT TO DELTA temp_delta1")
      val table1 = catalog.loadTable(Identifier.of(Array("default"), "temp_delta1"))
      assert(table1.properties().get("specific_type").equals("temporary"))
      assert(table1.asInstanceOf[DeltaTableV2].catalogTable.get.location
        .toASCIIString.contains("default.temp_delta1"))
      assert(table1.asInstanceOf[DeltaTableV2].catalogTable.get.tableType
        === CatalogTableType.TEMPORARY)

      sql(
        """
          |CREATE TEMPORARY TABLE temp_delta2 (id int, name string) using delta
          |""".stripMargin)
      val table2 = catalog.loadTable(Identifier.of(Array("default"), "temp_delta2"))
      assert(table2.properties().get("specific_type").equals("temporary"))
      assert(table2.asInstanceOf[DeltaTableV2].catalogTable.get.location
        .toASCIIString.contains("default.temp_delta2"))
      assert(table2.asInstanceOf[DeltaTableV2].catalogTable.get.tableType
        === CatalogTableType.TEMPORARY)
      sql("INSERT INTO TABLE temp_delta2 VALUES (1, 'a')")
      checkAnswer(spark.table("temp_delta2"), Row(1, "a") :: Nil)

      sql(
        """
          |CREATE TEMPORARY TABLE temp_delta3 using delta AS SELECT * FROM temp_delta2
          |""".stripMargin)
      val table3 = catalog.loadTable(Identifier.of(Array("default"), "temp_delta3"))
      assert(table3.properties().get("specific_type").equals("temporary"))
      assert(table3.asInstanceOf[DeltaTableV2].catalogTable.get.tableType
        === CatalogTableType.TEMPORARY)
      assert(table3.asInstanceOf[DeltaTableV2].catalogTable.get.location
        .toASCIIString.contains("default.temp_delta3"))
      checkAnswer(spark.table("temp_delta3"), Row(1, "a") :: Nil)
    }
  }

  private def append(deltaLog: DeltaLog, df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("delta").mode("append")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.save(deltaLog.dataPath.toString)
  }

  private def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  private def readParquetTable(path: String): DataFrame = {
    spark.read.format("parquet").load(path)
  }

  test("convert back a non-hive delta table") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.getCanonicalPath
    val deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
    try {
      append(deltaLog, Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      sql(s"CONVERT TO PARQUET delta.`$tempPath`")
      checkAnswer(readParquetTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    } finally {
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    }
  }

  test("convert back a non-partition delta table") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.getCanonicalPath
    val deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
    sql(
      s"""
         |create table test1 (key int, value int) using delta
         |location '$tempPath'
         |""".stripMargin)
    try {
      append(deltaLog, Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      checkAnswer(spark.table("test1"),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)

      sql(s"CONVERT TO PARQUET test1")
      checkAnswer(readParquetTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    } finally {
      sql("drop table if exists test1")
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    }
  }

  test("convert back a partition delta table") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.getCanonicalPath
    val deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
    sql(
      s"""
         |create table test2 (key int, value int) using delta
         |location '$tempPath'
         |partitioned by (value)
         |""".stripMargin)
    try {
      append(deltaLog, Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), Seq("VALUE"))
      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      checkAnswer(spark.table("test2"),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)

      sql(s"CONVERT TO PARQUET test2")
      checkAnswer(readParquetTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    } finally {
      sql("drop table if exists test2")
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    }
  }

  test("convert back a partition delta table without hive partition info " +
    "(simulate spark2.3 partition delta table") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.getCanonicalPath
    val deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
    sql(
      s"""
         |create table test3 (key int, value int) using parquet
         |partitioned by (value)
         |location '$tempPath'
         |""".stripMargin)
    try {
      // partitioned by 'value'
      append(deltaLog, Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), Seq("value"))
      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      val oldTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier("test3"))
      val newTable = oldTable.copy(provider = Some("delta"))
      spark.sessionState.catalog.alterTable(newTable)
      checkAnswer(spark.table("test3"),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
      var e = intercept[AnalysisException] {
        sql("SHOW PARTITIONS test3")
      }.getMessage()
      assert(e.contains("ShowPartitionsCommand not works on delta table"))

      sql(s"CONVERT TO PARQUET test3")
      checkAnswer(spark.table("test3"),
        Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    } finally {
      sql("drop table if exists test3")
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    }
  }

  test("convert back with rename policy") {
    val tempDir = Utils.createTempDir()
    val basePath = tempDir.getParent
    withSQLConf(DeltaSQLConf.VACUUM_RENAME_BASE_PATH.key -> basePath) {
      val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis())
      val datePath = new Path(conf.getConf(DeltaSQLConf.VACUUM_RENAME_BASE_PATH), currentDate)
      Utils.deleteRecursively(new File(datePath.toUri.toString))

      val tempPath = tempDir.getCanonicalPath
      val deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
      sql(
        s"""
           |create table test1 (key int, value int) using delta
           |location '$tempPath'
           |""".stripMargin)
      try {
        append(deltaLog, Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
        checkAnswer(readDeltaTable(tempPath),
          Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
        sql("DELETE FROM test1 WHERE key % 2 = 0")
        checkAnswer(spark.table("test1"), Row(1, 4) :: Row(1, 1) :: Nil)

        val allFiles = new File(tempPath).listFiles().map(_.getName)
          .filter(_.endsWith("parquet")).toSet

        sql("CONVERT TO PARQUET test1")
        checkAnswer(readParquetTable(tempPath), Row(1, 4) :: Row(1, 1) :: Nil)

        val restFiles = new File(tempPath).listFiles().map(_.getName)
          .filter(_.endsWith("parquet")).toSet

        val renamedFiles = new File(datePath.toUri.toString).listFiles().map(_.getName)
          .filter(_.endsWith("parquet")).toSet

        assert((renamedFiles ++ restFiles) === allFiles)
      } finally {
        sql("drop table if exists test1")
        Utils.deleteRecursively(tempDir)
        DeltaLog.clearCache()
      }
    }
  }

  test("show create table with different partition order") {
    withTable("test", "source") {
      sql("create table source (id int, type int, date string) using parquet")
      sql("insert into table source values (1, 1, 'a')")
      sql("insert into table source values (2, 2, 'b')")
      sql("insert into table source values (3, 1, 'b')")
      sql("insert into table source values (4, 2, 'a')")
      sql(
        """
          |create table test using delta partitioned by (date, type)
          |as select * from source
          |""".stripMargin)
      val expected =
        """CREATE TABLE `default`.`test` (
          |  `id` INT,
          |  `date` STRING,
          |  `type` INT)
          |USING delta
          |PARTITIONED BY (date, type)
          |""".stripMargin
      assert(sql("show create table test").head(10).head.getString(0) == expected)

    }
  }

  test("test execute compact non-partition delta table command") {
    withTable("table1") {
      withSQLConf(SQLConf.SMALL_FILE_SIZE_THRESHOLD.key -> "-1") {
        (1 to 100).map(i => (i, s"${i % 5}")).toDF("a", "b").
          repartition(3).write.saveAsTable("table1")
        sql("convert to delta table1")
        sql("update table1 set a = 0 where a % 5 = 0")
        sql("delete from table1 where a % 3 = 0 || a % 7 = 0")
        sql("delete from table1 where b = 0")
        val table = TableIdentifier("table1", Some("default"))
        val loc1 = spark.sessionState.catalog.getTableMetadata(table).location
        // scalastyle:off hadoopconfiguration
        val fs = FileSystem.get(loc1, spark.sparkContext.hadoopConfiguration)
        // scalastyle:one hadoopconfiguration
        val files1 = fs.listStatus(new Path(loc1.getPath))
          .filterNot(_.getPath.toString.contains("_SUCCESS"))
          .filterNot(_.getPath.toString.contains("_delta_log"))
        assert(files1.length == 9)
        checkAnswer(spark.table("table1"),
          Seq(Row(1, "1"), Row(2, "2"), Row(21, "1"), Row(22, "2"), Row(23, "3"), Row(42, "2"),
            Row(43, "3"), Row(44, "4"), Row(63, "3"), Row(64, "4"), Row(84, "4"), Row(86, "1")))
        sql("compact table table1 into 1 files")
        val afterCompact = spark.sessionState.catalog.getTableMetadata(table)
        val loc2 = afterCompact.location
        assert(loc1.getPath == loc2.getPath)
        val files2 = fs.listStatus(new Path(loc2.getPath))
          .filterNot(_.getPath.toString.contains("_SUCCESS"))
          .filterNot(_.getPath.toString.contains("_delta_log"))
        assert(files2.length == 1)
        assert(afterCompact.provider.contains("delta"))

        checkAnswer(spark.table("table1"),
          Seq(Row(1, "1"), Row(2, "2"), Row(21, "1"), Row(22, "2"), Row(23, "3"), Row(42, "2"),
            Row(43, "3"), Row(44, "4"), Row(63, "3"), Row(64, "4"), Row(84, "4"), Row(86, "1")))
      }
    }
  }

  test("test execute compact all partitions delta table command") {
    withTable("table2") {
      withSQLConf(SQLConf.SMALL_FILE_SIZE_THRESHOLD.key -> "-1") {
        (1 to 100).map(i => (i, s"${i % 5}")).toDF("a", "b").
          repartition(3).write.format("parquet").
          partitionBy("b").saveAsTable("table2")
        sql("convert to delta table2")
        sql("update table2 set a = 0 where a % 5 = 0")
        sql("delete from table2 where a % 3 = 0 || a % 7 = 0")
        sql("delete from table2 where b = '0'")
        val table = TableIdentifier("table2", Some("default"))
        val loc1 = spark.sessionState.catalog.getTableMetadata(table).location
        // scalastyle:off hadoopconfiguration
        val fs = FileSystem.get(loc1, spark.sparkContext.hadoopConfiguration)
        // scalastyle:on hadoopconfiguration
        assert(fs.listStatus(new Path(loc1.getPath))
          .filterNot(_.getPath.toString.contains("_SUCCESS"))
          .filterNot(_.getPath.toString.contains("_delta_log")).length == 5)
        val it = fs.listFiles(new Path(loc1), true)
        val allFiles = new ArrayBuffer[LocatedFileStatus]
        while(it.hasNext) {
          val e = it.next()
          if (!e.getPath.toString.contains("_SUCCESS") &&
            !e.getPath.toString.contains("_delta_log")) {
            allFiles += e
          }
        }
        assert(allFiles.length == 24)

        sql("compact table table2 into 1 files")
        val afterCompact = spark.sessionState.catalog.getTableMetadata(table)
        val loc2 = afterCompact.location
        assert(loc1.getPath == loc2.getPath)
        assert(fs.listStatus(new Path(loc2.getPath))
          .filterNot(_.getPath.toString.contains("_SUCCESS"))
          .filterNot(_.getPath.toString.contains("_delta_log")).length == 5)
        val it2 = fs.listFiles(new Path(loc2), true)
        val allFiles2 = new ArrayBuffer[LocatedFileStatus]
        while(it2.hasNext) {
          val e = it2.next()
          if (!e.getPath.toString.contains("_SUCCESS") &&
            !e.getPath.toString.contains("_delta_log")) {
            allFiles2 += e
          }
        }
        assert(allFiles2.length == 4)
        assert(afterCompact.provider.contains("delta"))
        checkAnswer(sql("select * from table2"),
          Seq(Row(1, "1"), Row(2, "2"), Row(21, "1"), Row(22, "2"), Row(23, "3"), Row(42, "2"),
            Row(43, "3"), Row(44, "4"), Row(63, "3"), Row(64, "4"), Row(84, "4"), Row(86, "1")))
      }
    }
  }

  test("test execute compact specified partition table command") {
    withTable("table3") {
      withSQLConf(SQLConf.SMALL_FILE_SIZE_THRESHOLD.key -> "-1") {
        (1 to 100).map(i => (i, s"${i % 5}")).toDF("a", "b").
          repartition(3).write.format("parquet").
          partitionBy("b").saveAsTable("table3")
        sql("convert to delta table3")
        sql("update table3 set a = 0 where a % 5 = 0")
        sql("delete from table3 where a % 3 = 0 || a % 7 = 0")
        sql("delete from table3 where b = '0'")
        val table = TableIdentifier("table3", Some("default"))
        val loc1 = spark.sessionState.catalog.getTableMetadata(table).location
        // scalastyle:off hadoopconfiguration
        val fs = FileSystem.get(loc1, spark.sparkContext.hadoopConfiguration)
        // scalastyle:on hadoopconfiguration
        assert(fs.listStatus(new Path(loc1.getPath))
          .filterNot(_.getPath.toString.contains("_SUCCESS"))
          .filterNot(_.getPath.toString.contains("_delta_log")).length == 5)
        val it = fs.listFiles(new Path(loc1), true)
        val allFiles = new ArrayBuffer[LocatedFileStatus]
        while(it.hasNext) {
          val e = it.next()
          if (!e.getPath.toString.contains("_SUCCESS") &&
            !e.getPath.toString.contains("_delta_log")) {
            allFiles += e
          }
        }
        assert(allFiles.length == 24)

        sql("compact table table3 partition(b='1') into 1 files")
        val afterCompact = spark.sessionState.catalog.getTableMetadata(table)
        val loc2 = afterCompact.location
        assert(loc1.getPath == loc2.getPath)
        assert(fs.listStatus(new Path(loc2.getPath))
          .filterNot(_.getPath.toString.contains("_SUCCESS"))
          .filterNot(_.getPath.toString.contains("_delta_log")).length == 5)
        val it2 = fs.listFiles(new Path(loc2), true)
        val allFiles2 = new ArrayBuffer[LocatedFileStatus]
        while(it2.hasNext) {
          val e = it2.next()
          if (!e.getPath.toString.contains("_SUCCESS") &&
            !e.getPath.toString.contains("_delta_log")) {
            allFiles2 += e
          }
        }
        assert(allFiles2.length == 7)
        assert(afterCompact.provider.contains("delta"))
        checkAnswer(sql("select * from table3"),
          Seq(Row(1, "1"), Row(2, "2"), Row(21, "1"), Row(22, "2"), Row(23, "3"), Row(42, "2"),
            Row(43, "3"), Row(44, "4"), Row(63, "3"), Row(64, "4"), Row(84, "4"), Row(86, "1")))
      }
    }
  }

  test("test convert to delta partition purge") {
    withTable("target") {
      sql(
        """
          |create table target (id int, dt string) using parquet
          |partitioned by (dt)
          |""".stripMargin)
      sql("insert into table target values (1, 'a'), (2, 'b'), (3, 'c')")
      val partsBeforeConvert = spark.sessionState.catalog.listPartitions(TableIdentifier("target"))
      assert(partsBeforeConvert.size == 3)
      sql("convert to delta target")
      Thread.sleep(5000) // since drop partition async
      val partsAfterConvert = spark.sessionState.catalog.listPartitions(TableIdentifier("target"))
      assert(partsAfterConvert.size == 0)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("target"))
      val deltaLog = DeltaLog.forTable(spark, table)
      assert(deltaLog.snapshot.listPartitions(table).size == 3)
      sql("convert to parquet target")
      val partsConvertBack = spark.sessionState.catalog.listPartitions(TableIdentifier("target"))
      assert(partsConvertBack.size == 3)
    }
  }

  test("Test convert partition with __HIVE_DEFAULT_PARTITION__") {
    withTable("test1", "test2") {
      sql("create table test1 (id int, dt string) using delta partitioned by (dt)")
      sql("insert into table test1 values (1, '__HIVE_DEFAULT_PARTITION__')")
      // todo not support in 3.0 yet
//      checkAnswer(
//        sql("SHOW PARTITIONS test1"),
//        Row("dt=__HIVE_DEFAULT_PARTITION__") :: Nil
//      )
      sql("convert to parquet test1") // no NPE
      checkAnswer(
        sql("SHOW PARTITIONS test1"),
        Row("dt=__HIVE_DEFAULT_PARTITION__") :: Nil
      )

      sql("create table test2 (id int, dt string) using delta partitioned by (dt)")
      sql("insert into table test2 values (1, null)")
      // todo not support in 3.0 yet
//      checkAnswer(
//        sql("SHOW PARTITIONS test2"),
//        Row("dt=__HIVE_DEFAULT_PARTITION__") :: Nil
//      )
      sql("convert to parquet test2") // no NPE
      checkAnswer(
        sql("SHOW PARTITIONS test2"),
        Row("dt=__HIVE_DEFAULT_PARTITION__") :: Nil
      )
    }
  }

  test("Test legacy empty delta table") {
    withTable("test1", "test2") {
      sql("create table test1 (id int, name string) using delta")
      sql("create table test2 (id int, name string, dt string) using delta partitioned by (dt)")
      val logPath1 = DeltaLog.forTable(spark, TableIdentifier("test1")).logPath
      val fs = logPath1.getFileSystem(spark.sessionState.newHadoopConf)
      fs.delete(logPath1, true)
      val logPath2 = DeltaLog.forTable(spark, TableIdentifier("test2")).logPath
      fs.delete(logPath2, true)
      val test1 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("test1"))
      val mockLegacyTest1 = test1.copy(createVersion = DeltaSourceUtils.LEGACY_TABLE_CREATED_BY)
      spark.sessionState.catalog.alterTable(mockLegacyTest1)
      val qualified1 = QualifiedTableName(test1.database, test1.identifier.table)
      spark.sessionState.catalog.invalidateCachedTable(qualified1)
      val test2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("test2"))
      val mockLegacyTest2 = test2.copy(createVersion = DeltaSourceUtils.LEGACY_TABLE_CREATED_BY)
      spark.sessionState.catalog.alterTable(mockLegacyTest2)
      val qualified2 = QualifiedTableName(test2.database, test2.identifier.table)
      spark.sessionState.catalog.invalidateCachedTable(qualified2)

      var msg = intercept[AnalysisException] {
        sql("update test1 set id = 5")
      }.getMessage
      assert(msg.contains("Any operation on the table via current Spark version is not allowed"))
      msg = intercept[AnalysisException] {
        sql("update test2 set id = 5")
      }.getMessage
      assert(msg.contains("Any operation on the table via current Spark version is not allowed"))
    }
  }
}
