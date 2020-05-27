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

class DeltaTableListener extends SparkListener with Logging {
  private val spark = SparkSession.getDefaultSession.get

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: ConvertToDeltaEvent =>
        DeltaTableMetadata.insertIntoMetadataTable(spark, e.metadata)
      case e: UpdateDeltaEvent =>
        DeltaTableMetadata.updateMetadataTable(spark, e.metadata)
      case e: DeleteDeltaEvent =>
        DeltaTableMetadata.deleteFromMetadataTable(spark, e.metadata)
      case e: RenameTableEvent =>
        val searchCondition = new DeltaTableMetadata(e.database, e.name)
        DeltaTableMetadata.selectFromMetadataTable(spark, searchCondition).foreach { old =>
          val newTableIdent = TableIdentifier(e.newName, Some(e.database))
          val newTable = spark.sessionState.catalog.getTableMetadata(newTableIdent)
          val newMetadata = DeltaTableMetadata(e.database, e.newName,
            old.maker, CatalogUtils.URIToString(newTable.location), old.vacuum, old.retention)
          DeltaTableMetadata.updateMetadataTable(spark, newMetadata, searchCondition)
        }
      case e: DropTableEvent =>
        val searchCondition = new DeltaTableMetadata(e.database, e.name)
        if (DeltaTableMetadata.metadataTableExists(spark, searchCondition)) {
          DeltaTableMetadata.deleteFromMetadataTable(spark, searchCondition)
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
