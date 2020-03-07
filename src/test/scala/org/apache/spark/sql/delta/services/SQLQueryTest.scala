package org.apache.spark.sql.delta.services

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}

class SQLQuerySuite extends QueryTest
  with SharedSparkSession with DeltaHiveSQLCommandTest with SQLTestUtils {

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
}
