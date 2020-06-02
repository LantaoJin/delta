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

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogUtils, DropTableEvent, RenameTableEvent, TableEvent}
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.util.ThreadUtils

class DeltaTableListener(validate: ValidateTask) extends SparkListener with Logging {
  private lazy val spark = SparkSession.active

  private lazy val metaHandlers =
    ThreadUtils.newDaemonFixedThreadPool(16, "delta-meta-table-handler-pool")

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: ConvertToDeltaEvent =>
        validate.enableVacuum(e.metadata)
        metaHandlers.execute(new Runnable {
          override def run(): Unit = {
            DeltaTableMetadata.insertIntoMetadataTable(spark, e.metadata)
          }
        })
      case e: UpdateDeltaEvent =>
        if (validate.deltaTableToVacuumTask.contains(e.metadata)) {
          validate.deltaTableToVacuumTask(e.metadata).foreach(_.cancel(true))
          validate.deltaTableToVacuumTask.remove(e.metadata)
        }
        metaHandlers.execute(new Runnable {
          override def run(): Unit = {
            DeltaTableMetadata.updateMetadataTable(spark, e.metadata)
          }
        })
      case e: DeleteDeltaEvent =>
        if (validate.deltaTableToVacuumTask.contains(e.metadata)) {
          validate.deltaTableToVacuumTask(e.metadata).foreach(_.cancel(true))
          validate.deltaTableToVacuumTask.remove(e.metadata)
        }
        metaHandlers.execute(new Runnable {
          override def run(): Unit = {
            DeltaTableMetadata.deleteFromMetadataTable(spark, e.metadata)
          }
        })
      case e: RenameTableEvent =>
        val searchCondition = new DeltaTableMetadata(e.database, e.name)
        if (validate.deltaTableToVacuumTask.contains(searchCondition)) {
          val old = validate.deltaTableToVacuumTask.keySet.find(_.equals(searchCondition)).get
          validate.deltaTableToVacuumTask(searchCondition).foreach(_.cancel(true))
          validate.deltaTableToVacuumTask.remove(searchCondition)
          val newTableIdent = TableIdentifier(e.newName, Some(e.database))
          val newTable = spark.sessionState.catalog.getTableMetadata(newTableIdent)
          val newMetadata = DeltaTableMetadata(e.database, e.newName,
            old.maker, CatalogUtils.URIToString(newTable.location), old.vacuum, old.retention)
          validate.enableVacuum(newMetadata)
          metaHandlers.execute(new Runnable {
            override def run(): Unit = {
              DeltaTableMetadata.updateMetadataTable(spark, newMetadata, searchCondition)
            }
          })
        } else {
          val catalog = spark.sessionState.catalog
          val table = catalog.getTableMetadata(TableIdentifier(e.name, Some(e.database)))
          if (DeltaTableUtils.isDeltaTable(table)) {
            DeltaTableMetadata.selectFromMetadataTable(spark, searchCondition).foreach { oldMeta =>
              val newTableIdent = TableIdentifier(e.newName, Some(e.database))
              val newTable = catalog.getTableMetadata(newTableIdent)
              val newMetadata = DeltaTableMetadata(e.database, e.newName, oldMeta.maker,
                CatalogUtils.URIToString(newTable.location), oldMeta.vacuum, oldMeta.retention)
              metaHandlers.execute(new Runnable {
                override def run(): Unit = {
                  DeltaTableMetadata.updateMetadataTable(spark, newMetadata, searchCondition)
                }
              })
            }
          }
        }
      case e: DropTableEvent =>
        val searchCondition = new DeltaTableMetadata(e.database, e.name)
        if (validate.deltaTableToVacuumTask.contains(searchCondition)) {
          validate.deltaTableToVacuumTask(searchCondition).foreach(_.cancel(true))
          validate.deltaTableToVacuumTask.remove(searchCondition)
        }
        val table =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(e.name, Some(e.database)))
        if (DeltaTableUtils.isDeltaTable(table)) {
          metaHandlers.execute(new Runnable {
            override def run(): Unit = {
              DeltaTableMetadata.deleteFromMetadataTable(spark, searchCondition)
            }
          })
        }
      case _ =>
    }
  }
}

object DeltaTableListener {
  val DELTA_MANAGEMENT_QUEUE = "deltaManagement"
}

trait DeltaMetaEvent extends TableEvent

case class ConvertToDeltaEvent(metadata: DeltaTableMetadata) extends DeltaMetaEvent {
  override val name: String = metadata.tbl
  override val database: String = metadata.db
}

case class UpdateDeltaEvent(metadata: DeltaTableMetadata) extends DeltaMetaEvent {
  override val name: String = metadata.tbl
  override val database: String = metadata.db
}

case class DeleteDeltaEvent(metadata: DeltaTableMetadata) extends DeltaMetaEvent {
  override val name: String = metadata.tbl
  override val database: String = metadata.db
}
