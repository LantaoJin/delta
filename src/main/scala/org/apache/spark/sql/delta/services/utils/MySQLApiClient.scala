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

package org.apache.spark.sql.delta.services.utils

import java.sql.{Connection, ResultSet, Statement}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.commons.dbcp.{BasicDataSource, DelegatingResultSet}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.services.config

class MySQLApiClient(conf: SparkConf) extends Logging {

  val ds = {
    val bds = new BasicDataSource()
    var conn: Connection = null
    bds.setDriverClassName("com.mysql.jdbc.Driver")
    bds.setUrl(conf.get(config.MYSQL_ENDPOINT))
    bds.setUsername(conf.get(config.MYSQL_USERNAME))
    bds.setPassword(conf.get(config.MYSQL_PASSWORD))
    bds.setMaxActive(conf.get(config.MYSQL_THREADPOOL_MAX_NUM))
    bds.setInitialSize(conf.get(config.MYSQL_THREADPOOL_INIT_NUM))
    bds.setMinEvictableIdleTimeMillis(conf.get(config.MYSQL_THREADPOOL_MAX_IDLE_TIME))
    try {
      conn = bds.getConnection
    } catch {
      case e: Exception =>
        logError(s"Fail to test connection, error msg: ${e.getMessage}")
        throw e
    } finally {
      if (conn != null) conn.close()
    }
    bds
  }
  val jsonMapper = {
    val module = new SimpleModule
    module.addSerializer(classOf[DelegatingResultSet], ResultSetSerializer)

    val objectMapper = new ObjectMapper
    objectMapper.registerModule(module)
    objectMapper
  }

  def searchQueryLog(username: String, startFrom: String, startEnd: String, state: String,
                     queue: String, queryId: String, limit: Integer, skip: Integer): String = {
    var sql = "SELECT queryid, sessionid, username, start, duration, state, " +
      "queue, cost, outputtofile, rowcount, statement from ql_query"
    var countSql = "SELECT COUNT(*) as count from ql_query"

    var filter: String = ""
    if (username != null) filter += s"username='${username}' AND "
    // startFrom and startEnd should be timestamp in micro second, transfer it to second
    if (startFrom != null) filter += s"start>=from_unixtime(${startFrom.substring(0, 10)}) AND "
    if (startEnd != null) filter += s"start<=from_unixtime(${startEnd.substring(0, 10)}) AND "
    if (state != null) filter += s"state='${state}' AND "
    if (queue != null) filter += s"queue='${queue}' AND "
    if (queryId != null) filter += s"queryid='${queryId}' AND "

    if (!filter.equals("")) {
      filter = filter.substring(0, filter.length - 4)
      sql += " WHERE " + filter
      countSql += " WHERE " + filter
    }


    val limitRow = if (limit == null) 100 else limit
    val skipRow = if (skip == null) 0 else skip
    sql += s" order by start desc LIMIT ${skipRow}, ${limitRow}"

    val rs = executeSQL(sql)
    val rsCount = executeSQL(countSql)
    s"""{\"total\":$rsCount, \"result\":$rs}"""
  }

  def executeSQL(sql: String): String = {
    val conn = getConnection()
    var stmt: Statement = null
    var rs: ResultSet = null
    if (conn == null) {
      logError("Fail to get connection")
      new ObjectNode(JsonNodeFactory.instance)
    }
    try {
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      jsonMapper.writeValueAsString(rs)
    } catch {
      case e: Exception =>
        logError(s"Fail to execute ${sql}, error msg: ${e.getMessage}")
        "{}"
    } finally {
      closeConn(conn, stmt, rs)
    }
  }

  def getConnection(): Connection = {
    var conn: Connection = null
    try {
      conn = ds.getConnection()
    } catch {
      case e: Exception =>
        logError(s"Fail to get connection from pool, error msg: ${e.getMessage}")
    }
    conn
  }

  def closeConn(conn: Connection, stmt: Statement, rs: ResultSet): Unit = {
    try {
      if (conn != null) conn.close()
    } catch {
      case e: Exception =>
        logError(s"Fail to close connection, error msg: ${e.getMessage}")
    }
    try {
      if (rs != null) rs.close()
    } catch {
      case e: Exception =>
        logError(s"Fail to close result set, error msg: ${e.getMessage}")
    }
    try {
      if (stmt != null) stmt.close()
    } catch {
      case e: Exception =>
        logError(s"Fail to close prepare statement, error msg: ${e.getMessage}")
    }
  }
}

