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

import javax.servlet.http.HttpServletRequest
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.services.DeltaTableMetadata
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class DeltaTablesPage(parent: DeltaTab) extends WebUIPage("") with Logging {

  def deltaTables: Seq[DeltaTableMetadata] = {
    parent.validate.deltaTableToVacuumTask.keySet.toSeq
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      new AllTables(parent, deltaTables.sortBy(_.db), parent.validate.lastUpdatedTime).toNodeSeq
    UIUtils.headerSparkPage("Delta Tables", content, parent)
  }
}
