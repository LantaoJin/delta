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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.services.utils.JdbcHelper

/**
 * The metadata entities which contained in [[DELTA_META_TABLE_IDENTIFIER]]
 */
case class DeltaTableMetadata(
    db: String, tbl: String, maker: String, path: String,
    vacuum: Boolean, retention: Long) extends Ordered [DeltaTableMetadata] {

  private def this(db: String, tbl: String) {
    this(db, tbl, "", "", false, 12L) // 12L is useless, but better use a value greater than 0
  }

  override def hashCode: scala.Int = {
    31 * db.hashCode() + tbl.hashCode()
  }

  override def equals(that: Any): Boolean = that match {
    case t: DeltaTableMetadata => t.db == db && t.tbl == tbl
    case _ => false
  }

  override def compare(that: DeltaTableMetadata): Int = {
    if (this.db.isEmpty && that.db.nonEmpty) {
      -1
    } else if (this.db.nonEmpty && that.db.isEmpty) {
      1
    } else if (this.db.isEmpty && that.db.isEmpty) {
      Ordering.String.compare(this.tbl, that.tbl)
    } else {
      if (Ordering.String.compare(this.db, that.db) == 0) {
        Ordering.String.compare(this.tbl, that.tbl)
      } else {
        Ordering.String.compare(this.db, that.db)
      }
    }
  }

  override def toString: String = {
    if (vacuum) {
      s"Delta Table ${identifier.unquotedString} with vacuum retention $retention"
    } else {
      s"Delta Table ${identifier.unquotedString} without vacuum"
    }
  }

  def identifier: TableIdentifier = TableIdentifier(tbl, Option(db))
}


// ----------------------------------- //
//                 API                 //
// ----------------------------------- //
object DeltaTableMetadata extends Logging {

  def buildSearchCondition(db: String, tbl: String): DeltaTableMetadata = {
    new DeltaTableMetadata(db, tbl)
  }

  def deltaMetaTableIdentifier(conf: SparkConf): String = {
    conf.get(config.META_TABLE_IDENTIFIER)
  }

  def deltaMetaTableIdentifier(spark: SparkSession): String = {
    spark.sessionState.conf.getConf(config.META_TABLE_IDENTIFIER)
  }

  /**
   * SELECT * FROM DELTA_META_TABLE
   */
  def listMetadataTables(spark: SparkSession): Seq[DeltaTableMetadata] = seqCatch {
    val sqlText =
      s"""
         |SELECT * FROM ${deltaMetaTableIdentifier(spark)}
         |""".stripMargin
    logDebug(s"DeltaTableMetadata API execute: \n $sqlText")
    executeQuery(spark, sqlText).map(rowToObject)
  }

  /**
   * Get rows from DELTA_META_TABLE
   */
  def getRowsFromMetadataTable(spark: SparkSession): Seq[Row] = seqCatch {
    val sqlText =
      s"""
         |SELECT * FROM ${deltaMetaTableIdentifier(spark)}
         |""".stripMargin
    logDebug(s"DeltaTableMetadata API execute: \n $sqlText")
    executeQuery(spark, sqlText)
  }

  /**
   * SELECT * FROM DELTA_META_TABLE WHERE db=database and tbl=tablename
   */
  def selectFromMetadataTable(
      spark: SparkSession, metadata: DeltaTableMetadata): Option[DeltaTableMetadata] = {
    val sqlText =
      s"""
         |SELECT * FROM ${deltaMetaTableIdentifier(spark)}
         |WHERE
         |${toWheres(metadata)}
         |""".stripMargin
    logDebug(s"DeltaTableMetadata API execute: \n $sqlText")
    executeQuery(spark, sqlText).map(rowToObject).headOption
  }

  /**
   * Check delta table exists in DELTA_META_TABLE
   */
  def metadataTableExists(
      spark: SparkSession, metadata: DeltaTableMetadata): Boolean = booleanCatch {
    selectFromMetadataTable(spark, metadata).nonEmpty
  }

  /**
   * INSERT INTO DELTA_META_TABLE
   */
  def insertIntoMetadataTable(
      spark: SparkSession, metadata: DeltaTableMetadata): Boolean = booleanCatch {
    val sqlText =
      s"""
        |INSERT INTO ${deltaMetaTableIdentifier(spark)}
        |VALUES (${toValues(metadata)})
        |""".stripMargin
    logInfo(s"DeltaTableMetadata API execute: \n $sqlText")
    executeUpdate(spark, sqlText)
    true
  }

  /**
   * UPDATE DELTA_META_TABLE VALUES
   */
  def updateMetadataTable(
      spark: SparkSession, metadata: DeltaTableMetadata): Boolean = booleanCatch {
    val sqlText =
      s"""
         |UPDATE ${deltaMetaTableIdentifier(spark)}
         |SET
         |${toSets(metadata)}
         |WHERE
         |${toWheres(metadata)}
         |""".stripMargin
    logInfo(s"DeltaTableMetadata API execute: \n $sqlText")
    executeUpdate(spark, sqlText)
    true
  }

  /**
   * UPDATE DELTA_META_TABLE VALUES
   */
  def updateMetadataTable(
      spark: SparkSession,
      metadata: DeltaTableMetadata,
      search: DeltaTableMetadata): Boolean = booleanCatch {
    val sqlText =
      s"""
         |UPDATE ${deltaMetaTableIdentifier(spark)}
         |SET
         |${toSets(metadata)}
         |WHERE
         |${toWheres(search)}
         |""".stripMargin
    logInfo(s"DeltaTableMetadata API execute: \n $sqlText")
    executeUpdate(spark, sqlText)
    true
  }

  /**
   * DELETE FROM DELTA_META_TABLE VALUES
   */
  def deleteFromMetadataTable(
      spark: SparkSession, metadata: DeltaTableMetadata): Boolean = booleanCatch {
    val sqlText =
      s"""
         |DELETE FROM ${deltaMetaTableIdentifier(spark)}
         |WHERE
         |${toWheres(metadata)}
         |""".stripMargin
    logInfo(s"DeltaTableMetadata API execute: \n $sqlText")
    executeUpdate(spark, sqlText)
    true
  }

  def asMetadata(table: TableIdentifier): DeltaTableMetadata = {
    new DeltaTableMetadata(table.database.getOrElse(""), table.table)
  }

  private def booleanCatch(f: => Boolean): Boolean = {
    try {
      f
    } catch {
      case e: Throwable =>
        logWarning("", e)
        false
    }
  }

  private def seqCatch[T](f: => Seq[T]): Seq[T] = {
    try {
      f
    } catch {
      case e: Throwable =>
        logWarning("", e)
        Seq.empty[T]
    }
  }

  def toValues(m: DeltaTableMetadata): String = {
    s"'${m.db}', '${m.tbl}', '${m.maker}', '${m.path}', ${m.vacuum}, ${m.retention}"
  }

  def toSets(m: DeltaTableMetadata): String = {
    s"""  db='${m.db}',
       |  tbl='${m.tbl}',
       |  maker='${m.maker}',
       |  path='${m.path}',
       |  vacuum=${m.vacuum},
       |  retention=${m.retention}""".stripMargin
  }

  def toWheres(m: DeltaTableMetadata): String = {
    s"  db='${m.db}' and tbl='${m.tbl}'"
  }

  def rowToObject(row: Row): DeltaTableMetadata = {
    val db = row.getString(0)
    val tbl = row.getString(1)
    val maker = row.getString(2)
    val path = row.getString(3)
    val vacuum = row.getBoolean(4)
    val retention = row.getLong(5)
    DeltaTableMetadata(db, tbl, maker, path, vacuum, retention)
  }

  private def executeUpdate(spark: SparkSession, sqlText: String): Unit = {
    val conf = spark.sessionState.conf
    val storage = conf.getConf(config.META_TABLE_STORAGE)
    if (storage.equalsIgnoreCase("jdbc")) {
      JdbcHelper.updateOrDelete(spark, sqlText)
    } else {
      spark.sql(sqlText)
    }
  }

  private def executeQuery(spark: SparkSession, sqlText: String): Seq[Row] = {
    val conf = spark.sessionState.conf
    val storage = conf.getConf(config.META_TABLE_STORAGE)
    if (storage.equalsIgnoreCase("jdbc")) {
      JdbcHelper.select(spark, sqlText)
    } else {
      spark.sql(sqlText).collect().toSeq
    }
  }
}
