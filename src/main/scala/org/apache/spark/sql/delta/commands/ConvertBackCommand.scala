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

package org.apache.spark.sql.delta.commands

import java.util.Locale

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.delta.services.{ConvertToParquetEvent, DeltaTableMetadata}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableAddRangePartitionCommand, CommandUtils, DDLUtils}
import org.apache.spark.sql.types.StructType

case class ConvertBackCommand(
    tableIdentifier: TableIdentifier,
    partitionSchema: Option[StructType], // always None
    deltaPath: Option[String]) // always None
  extends ConvertToDeltaCommandBase(tableIdentifier, partitionSchema, deltaPath) {

  override def run(spark: SparkSession): Seq[Row] = {
    val convertProperties = getConvertProperties(spark, tableIdentifier)

    convertProperties.provider match {
      case Some(providerName) => providerName.toLowerCase(Locale.ROOT) match {
        case checkProvider if checkProvider != "delta" =>
          throw DeltaErrors.convertBackNonDeltaTablesException(tableIdentifier, checkProvider)
        case _ =>
      }
      case None =>
        throw DeltaErrors.missingProviderForConvertException(convertProperties.targetDir)
    }

    val deltaLog = if (convertProperties.catalogTable.isDefined) {
      DeltaLog.forTable(spark, convertProperties.catalogTable.get)
    } else {
      DeltaLog.forTable(spark, convertProperties.targetDir)
    }

    try {
      deltaLog.checkLogDirectoryExist()
    } catch {
      case e: Exception =>
        throw new AnalysisException(s"'CONVERT TO PARQUET' is only supported on delta table, " +
          s"failure caused by ${e.getMessage}")
    }


    val txn = deltaLog.startTransaction()
    performConvertBack(spark, txn, convertProperties)
  }

  protected def performConvertBack(
      spark: SparkSession,
      txn: OptimisticTransaction,
      convertProperties: ConvertProperties): Seq[Row] = {

    val targetPath = new Path(convertProperties.targetDir)
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val fs = targetPath.getFileSystem(sessionHadoopConf)
    val qualifiedPath = fs.makeQualified(targetPath)
    val qualifiedDir = qualifiedPath.toString
    if (!fs.exists(qualifiedPath)) {
      throw DeltaErrors.pathNotExistsException(qualifiedDir)
    }

    if (convertProperties.catalogTable.isDefined) {
      val oldTable = convertProperties.catalogTable.get
      // spark2.3 may convert back a spark3.0 delta table, but spark3.0 delta table
      // doesn't store partition info in HMS, so we read them from delta metadata.
      // But we CANNOT change a non-partitioned table to partitioned table in HMS
      val compatiblePartitionSchema = if (oldTable.partitionSchema.nonEmpty) {
        oldTable.partitionSchema
      } else if (txn.metadata.partitionSchema.nonEmpty) {
        txn.metadata.partitionSchema
      } else {
        oldTable.partitionSchema
      }
      if (!compatiblePartitionSchema.equals(oldTable.partitionSchema)) {
        throw DeltaErrors.convertPartitionIncompatibleException(
          oldTable, compatiblePartitionSchema)
      }
    }

    // 1. clean all untracked files by vacuum
    VacuumCommand.gc(spark, txn.deltaLog, dryRun = false, Some(0), safetyCheckEnabled = false)

    if (convertProperties.catalogTable.isDefined) {
      val oldTable = convertProperties.catalogTable.get
      val newTable = oldTable.copy(provider = Some("parquet"), tracksPartitionsInCatalog = true)

      try {
        // todo (lajin) step 3 should execute before step 2.
        // but now we can not AddPartition to delta table, keep this as todo
        // 2. change provider to parquet. after this, spark reads it as parquet table
        spark.sessionState.catalog.alterTable(newTable)
        // 3. write partition metadata to hive metastore
        val partitionColumns = Try {
          txn.deltaLog.snapshot.listPartitions(newTable)
        } match {
          case Success(value) => value
          case Failure(exception) => Nil
        }
        if (partitionColumns.nonEmpty) {
          val isRangePartitionedTable = false // todo (lajin)
          if (isRangePartitionedTable) {
            val namedPartitions = partitionColumns.map { p =>
              val name = p.spec.values.head
              ((Some(name), p.spec), None)
            }
            AlterTableAddRangePartitionCommand(newTable.identifier,
              namedPartitions, ifNotExists = true).run(spark)
          } else {
            AlterTableAddPartitionCommand(newTable.identifier,
              partitionColumns.map(p => (p.spec, None)), ifNotExists = true).run(spark)
          }
        }
      } catch {
        case e: Throwable =>
          // rollback provider to delta
          spark.sessionState.catalog.alterTable(oldTable)
          throw e
      }
    }

    // 4. succeed to convert to parquet, safety delete delta log dir
    val store = txn.deltaLog.store
    store.delete(Seq(txn.deltaLog.logPath), recursive = true)
    DeltaLog.invalidateCache(spark, txn.deltaLog.dataPath)

    // 5. clean cache and refresh
    if (convertProperties.catalogTable.isDefined) {
      val newTable = spark.sessionState.catalog.getTableMetadata(
        convertProperties.catalogTable.get.identifier)
      val qualified = QualifiedTableName(newTable.database, newTable.identifier.table)
      spark.sessionState.catalog.invalidateCachedTable(qualified)
      spark.catalog.refreshTable(newTable.identifier.quotedString)
      CommandUtils.updateTableStats(spark, newTable)

      removeFromMetaTable(spark, convertProperties)
    }
    Seq.empty[Row]
  }

  private def removeFromMetaTable(
      spark: SparkSession, convertProperties: ConvertProperties): Unit = {
    convertProperties.catalogTable.foreach { table =>
      val searchCondition = DeltaTableMetadata.buildSearchCondition(
        table.identifier.database.getOrElse(""), table.identifier.table)
      val isTemp = DDLUtils.isTemporaryTable(table)
      spark.sharedState.externalCatalog.postToAll(ConvertToParquetEvent(searchCondition, isTemp))
    }
  }
}
