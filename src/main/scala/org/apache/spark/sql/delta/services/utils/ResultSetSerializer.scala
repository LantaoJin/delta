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

import java.sql.{Blob, Clob, ResultSet, Types}

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import org.apache.spark.internal.Logging

object ResultSetSerializer extends JsonSerializer[ResultSet] with Logging {


  override def handledType(): Class[ResultSet] = {
    classOf[ResultSet]
  }

  override def serialize(rs: ResultSet, jgen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    try {
      val rsmd = rs.getMetaData
      val numColumns = rsmd.getColumnCount
      val columnNames: Array[String] = new Array[String](numColumns)
      val columnTypes: Array[Int] = new Array[Int](numColumns)

      for (i <- 0 to numColumns - 1) {
        columnNames(i) = rsmd.getColumnLabel(i + 1)
        columnTypes(i) = rsmd.getColumnType(i + 1)
      }

      jgen.writeStartArray()

      while (rs.next()) {
        var b: Boolean = false
        var l: Long = 0
        var d: Double = 0.0

        jgen.writeStartObject()

        for (i <- 0 to numColumns - 1) {
          jgen.writeFieldName(columnNames(i))
          columnTypes(i) match {
            case Types.INTEGER =>
              l = rs.getInt(i + 1)
              if (rs.wasNull()) jgen.writeNull()
              else jgen.writeNumber(l)
            case Types.BIGINT =>
              l = rs.getLong(i + 1)
              if (rs.wasNull()) jgen.writeNull()
              else jgen.writeNumber(l)
            case Types.DECIMAL | Types.NUMERIC =>
              jgen.writeNumber(rs.getBigDecimal(i + 1))
            case Types.BOOLEAN | Types.BIT =>
              b = rs.getBoolean(i + 1)
              if (rs.wasNull()) jgen.writeNull()
              else jgen.writeBoolean(b)
            case Types.NVARCHAR | Types.VARCHAR | Types.LONGNVARCHAR | Types.LONGVARCHAR =>
              jgen.writeString(rs.getString(i + 1))
            case Types.FLOAT | Types.REAL | Types.DOUBLE =>
              d = rs.getDouble(i + 1)
              if (rs.wasNull()) jgen.writeNull()
              else jgen.writeNumber(d)
            case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY =>
              jgen.writeBinary(rs.getBytes(i + 1))
            case Types.TINYINT | Types.SMALLINT =>
              l = rs.getShort(i + 1)
              if (rs.wasNull()) jgen.writeNull()
              else jgen.writeNumber(l)
            case Types.DATE =>
              provider.defaultSerializeValue(rs.getObject(i + 1), jgen)
            case Types.TIMESTAMP =>
              provider.defaultSerializeValue(rs.getObject(i + 1), jgen)
            case Types.BLOB =>
              val blob: Blob = rs.getBlob(i)
              provider.defaultSerializeValue(blob.getBinaryStream(), jgen)
              blob.free()
            case Types.CLOB =>
              val clob: Clob = rs.getClob(i)
              provider.defaultSerializeValue(clob.getCharacterStream, jgen)
              clob.free()
            case Types.ARRAY =>
              throw new RuntimeException("ResultSetSerializer not yet implemented " +
                "for SQL type ARRAY");
            case Types.STRUCT =>
              throw new RuntimeException("ResultSetSerializer not yet implemented " +
                "for SQL type STRUCT");
            case Types.DISTINCT =>
              throw new RuntimeException("ResultSetSerializer not yet implemented " +
                "for SQL type DISTINCT");
            case Types.REF =>
              throw new RuntimeException("ResultSetSerializer not yet implemented " +
                "for SQL type REF");
            case _ => provider.defaultSerializeValue(rs.getObject(i + 1), jgen)
          }
        }
        jgen.writeEndObject()
      }
      jgen.writeEndArray()
    } catch {
      case e: Exception =>
        logError(s"Fail to parse type ${e.getMessage}")
    }
  }
}
