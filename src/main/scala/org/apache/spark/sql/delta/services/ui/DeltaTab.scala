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

import org.apache.spark.sql.delta.services.ValidateTask
import org.apache.spark.ui.{SparkUI, SparkUITab}

class DeltaTab(val validate: ValidateTask, parent: SparkUI) extends SparkUITab(parent, "Delta") {
  attachPage(new DeltaTablesPage(this))

  parent.attachTab(this)

  parent.addStaticHandler(DeltaTab.STATIC_RESOURCE_DIR, "/static/delta")
}

object DeltaTab {
  private val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
}
