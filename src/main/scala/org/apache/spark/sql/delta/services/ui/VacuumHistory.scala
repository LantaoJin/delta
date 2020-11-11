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

case class VacuumingInfo(
    db: String, tbl: String,
    filesDeleted: Long,
    start: String, end: String, lastDDLTime: Long)

class VacuumHistory(
    parent: DeltaTab,
    vacuuming: Seq[VacuumingInfo],
    title: String,
    complete: Boolean) {

  def header: Seq[String] =
    if (complete) {
      Seq(
        "database",
        "table",
        "filesDeleted",
        "start",
        "end")
    } else {
      Seq(
        "database",
        "table",
        "start")
    }

  def row(v: VacuumingInfo): Seq[Node] = {
    if (complete) {
      <tr>
        <td>
          {v.db}
        </td>
        <td>
          {v.tbl}
        </td>
        <td>
          {v.filesDeleted}
        </td>
        <td>
          {v.start}
        </td>
        <td>
          {v.end}
        </td>
      </tr>
    } else {
      <tr>
        <td>
          {v.db}
        </td>
        <td>
          {v.tbl}
        </td>
        <td>
          {v.start}
        </td>
      </tr>
    }

  }

  def toNodeSeq: Seq[Node] = {
    <div>
      <h4>{vacuuming.size} tables {title}</h4>
      {UIUtils.listingTable[VacuumingInfo](
      header, row, vacuuming, id = Some(title))}
    </div>
  }
}
