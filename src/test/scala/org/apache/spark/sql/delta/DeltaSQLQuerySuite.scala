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

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.util.Utils

class DeltaSQLQuerySuite extends QueryTest with SQLTestUtils with SharedSparkSession
  with DeltaSQLCommandTest {

  var tempDir: File = _

  var deltaLog: DeltaLog = _

  protected def tempPath: String = tempDir.getCanonicalPath

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
  }

  override def afterEach() {
    try {
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  test("test statistics") {
    withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      withTable("delta_table") {
        sql(
          s"""
            |CREATE TABLE delta_table(a INT)
            |USING delta
            |OPTIONS('path'='$tempPath')
            |""".stripMargin)
        val catalog = spark.sessionState.catalog
        var table = catalog.getTableMetadata(TableIdentifier("delta_table"))
        assert(table.stats.isEmpty)
        sql(
          """
            |INSERT INTO TABLE delta_table values (3)
            |""".stripMargin)
        checkAnswer(readDeltaTable(tempPath), Row(3) :: Nil)
        table = catalog.getTableMetadata(TableIdentifier("delta_table"))
        assert(table.stats.isDefined)
        val size1 = table.stats.get.sizeInBytes
        assert(size1 > 0)

        sql(
          """
            |INSERT INTO TABLE delta_table values (3)
            |""".stripMargin)
        checkAnswer(readDeltaTable(tempPath), Row(3) :: Row(3) ::Nil)
        table = catalog.getTableMetadata(TableIdentifier("delta_table"))
        assert(table.stats.isDefined)
        val size2 = table.stats.get.sizeInBytes
        assert(size1 < size2)
        sql(
          """
            |UPDATE delta_table SET a = 30 WHERE a = 3
            |""".stripMargin)
        checkAnswer(readDeltaTable(tempPath), Row(30) :: Row(30) :: Nil)
        table = catalog.getTableMetadata(TableIdentifier("delta_table"))
        assert(table.stats.isDefined)
        val size3 = table.stats.get.sizeInBytes
        assert(size2 == size3)

        sql(
          """
            |DELETE FROM delta_table WHERE a = 30
            |""".stripMargin)
        checkAnswer(readDeltaTable(tempPath), Nil)
        table = catalog.getTableMetadata(TableIdentifier("delta_table"))
        assert(table.stats.isDefined)
        val size4 = table.stats.get.sizeInBytes
        assert(size3 > size4)
      }
    }
  }
}
