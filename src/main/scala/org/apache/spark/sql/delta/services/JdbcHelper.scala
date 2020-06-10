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

import java.sql.ResultSet

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.viewpoint.client.MySQLApiClient

object JdbcHelper {

  private var client: MySQLApiClient = _

  def getJdbcClient(config: SparkConf): MySQLApiClient = {
    if (client == null) {
      client = new MySQLApiClient(config)
      client
    } else {
      client
    }
  }

  def updateOrDelete(spark: SparkSession, sql: String): Unit = {
    val config = spark.sparkContext.conf
    val client = getJdbcClient(config)
    val conn = client.getConnection()
    val statement = conn.createStatement
    try {
      statement.executeUpdate(sql)
    } finally {
      if (statement != null) {
        statement.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def select(spark: SparkSession, sql: String): Array[Row] = {
    val config = spark.sparkContext.conf
    val client = getJdbcClient(config)
    val conn = client.getConnection()
    val statement = conn.createStatement
    var rs: ResultSet = null
    try {
      rs = statement.executeQuery(sql)
      val schema = Encoders.product[DeltaTableMetadata].schema
      JdbcUtils.resultSetToRows(rs, schema).toArray
    } finally {
      if (rs != null && !rs.isClosed) {
        rs.close()
      }
      if (statement != null && !statement.isClosed) {
        statement.close()
      }
      if (conn != null && !conn.isClosed) {
        conn.close()
      }
    }
  }
}
