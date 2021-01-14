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

package org.apache.spark.sql.delta.services.ui

import scala.xml.Node

import org.apache.spark.ui.UIUtils

case class VacuumSkipped(
    db: String, tbl: String,
    var totalFiles: Long,
    var skippedTime: String,
    var reason: String) {

  def update(totalFiles: Long, skippedTime: String, reason: String): Unit = {
    if (totalFiles > 0L) {
      this.totalFiles = totalFiles
    }
    this.totalFiles = totalFiles
    this.skippedTime = skippedTime
    this.reason = reason
  }
}

class SkippedHistory(
    parent: DeltaTab,
    skipped: Seq[VacuumSkipped],
    title: String) {

  def header: Seq[String] =
    Seq(
      "database",
      "table",
      "totalFiles",
      "skipped time",
      "reason or comment")

  def row(v: VacuumSkipped): Seq[Node] = {
    <tr>
      <td>
        {v.db}
      </td>
      <td>
        {v.tbl}
      </td>
      <td>
        {if (v.totalFiles < 0) "NaN" else v.totalFiles.toString}
      </td>
      <td>
        {v.skippedTime}
      </td>
      <td>
        {v.reason}
      </td>
    </tr>
  }

  def toNodeSeq: Seq[Node] = {
    <div>
      <span class="collapse-aggregated-skipped collapse-table"
            onClick="collapseTable('collapse-aggregated-skipped','aggregated-skipped')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>{skipped.size} tables {title}</a>
        </h4>
      </span>
      <div class="aggregated-skipped collapsible-table">
        {UIUtils.listingTable[VacuumSkipped](
          header, row, skipped, id = Some(title))}
      </div>
    </div>
  }
}
