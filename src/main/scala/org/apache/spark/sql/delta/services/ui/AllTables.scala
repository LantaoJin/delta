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


package org.apache.spark.sql.delta.services.ui

import scala.xml.Node

import org.apache.spark.sql.delta.services.DeltaTableMetadata
import org.apache.spark.ui.UIUtils

class AllTables(
    parent: DeltaTab,
    deltaTables: Seq[DeltaTableMetadata],
    lastUpdatedTime: String) {


  def header: Seq[String] =
    Seq(
      "database",
      "table",
      "maker",
      "path",
      "vacuum",
      "retention")

  def row(meta: DeltaTableMetadata): Seq[Node] = {
    <tr>
      <td>
        {meta.db}
      </td>
      <td>
        {meta.tbl}
      </td>
      <td>
        {meta.maker}
      </td>
      <td>
        {meta.path}
      </td>
      <td>
        {meta.vacuum.toString}
      </td>
      <td>
        {meta.retention.toString + " hours"}
      </td>
    </tr>
  }

  def toNodeSeq: Seq[Node] = {
    <div>
      <h4>Delta Tables (Last Updated: {lastUpdatedTime})</h4>
      {UIUtils.listingTable[DeltaTableMetadata](
      header, row, deltaTables, id = Some("delta-table"))}
    </div>
  }
}
