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

import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.internal.SQLConf

/**
 * The metadata entities which contained in [[DELTA_META_TABLE_IDENTIFIER]]
 */
case class DeltaTableMetadata(
    db: String, tbl: String, maker: String, path: String,
    vacuum: Boolean, retention: Option[Long]) extends Ordered [DeltaTableMetadata] {

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
      s"Delta Table ${identifier.unquotedString} with vacuum retention ${retention.getOrElse(-1L)}"
    } else {
      s"Delta Table ${identifier.unquotedString} without vacuum"
    }
  }

  def identifier: TableIdentifier = TableIdentifier(tbl, Option(db))
}

object DeltaTableMetadata extends Logging {
  // --------------
  //      API
  // --------------

  def deltaMetaTableIdentifier(conf: SparkConf): String = {
    conf.get(DeltaSQLConf.META_TABLE_IDENTIFIER)
  }

  def deltaMetaTableLocation(conf: SQLConf): Option[String] = {
    conf.getConf(DeltaSQLConf.META_TABLE_LOCATION)
  }

  def createDataFrameOfDeltaMetaTable(spark: SparkSession): Option[DataFrame] = {
    deltaMetaTableLocation(spark.sessionState.conf).flatMap { location =>
      try {
        Some(spark.read.parquet(location))
      } catch {
        case e: Exception =>
          logWarning(s"Failed to create dataframe for Delta meta table from $location", e)
          None
      }
    }
  }

  def buildDeltaTableFromLocation(spark: SparkSession, location: String): DeltaTable = {
    DeltaTable.forPath(spark, location)
  }

  def deleteWithCondition(spark: SparkSession, condition: String): Boolean = {
    val deltaMetaTableName = deltaMetaTableIdentifier(spark.sparkContext.conf)
    val location = deltaMetaTableLocation(spark.sessionState.conf)
    if (location.isDefined) {
      try {
        DeltaTable.convertToDelta(spark, deltaMetaTableName)
        val deltaMetaTable = DeltaTable.forPath(spark, location.get)
        deltaMetaTable.delete(condition)
        true
      } catch {
        case _: Throwable => false
      }
    } else {
      false
    }
  }

  def deleteWithCondition(spark: SparkSession, database: String, table: String): Boolean = {
    deleteWithCondition(spark, s"db=$database and tbl=$table")
  }

  def asMetadata(table: TableIdentifier): DeltaTableMetadata = {
    DeltaTableMetadata(table.database.getOrElse(""), table.table, null, null, false, None)
  }
}
