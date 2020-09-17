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

import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils

case class VacuumingInfo(
    db: String, tbl: String,
    databaseQuotaUsage: Double,
    sizeBefore: Long, sizeAfter: Long,
    fileCountBefore: Long, fileCountAfter: Long,
    start: String, end: String)

class VacuumHistory(
    parent: DeltaTab,
    vacuuming: Seq[VacuumingInfo],
    title: String) {


  def header: Seq[String] =
    Seq(
      "database",
      "table",
      "dbQuotaUsage",
      "sizeBefore",
      "sizeAfter",
      "filesBefore",
      "filesAfter",
      "start",
      "end")

  def row(v: VacuumingInfo): Seq[Node] = {
    <tr>
      <td>
        {v.db}
      </td>
      <td>
        {v.tbl}
      </td>
      <td>
        {v.databaseQuotaUsage}%
      </td>
      <td>
        {Utils.bytesToString(v.sizeBefore)}
      </td>
      <td>
        {Utils.bytesToString(v.sizeAfter)}
      </td>
      <td>
        {v.fileCountBefore}
      </td>
      <td>
        {v.fileCountAfter}
      </td>
      <td>
        {v.start}
      </td>
      <td>
        {v.end}
      </td>
    </tr>
  }

  def toNodeSeq: Seq[Node] = {
    <div>
      <h4>{vacuuming.size} tables {title}</h4>
      {UIUtils.listingTable[VacuumingInfo](
      header, row, vacuuming, id = Some(title))}
    </div>
  }
}
