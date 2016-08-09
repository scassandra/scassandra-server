/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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
package org.scassandra.server.cqlmessages.response

import org.scassandra.server.cqlmessages._
import akka.util.{ByteStringBuilder, ByteIterator, ByteString}
import com.typesafe.scalalogging.LazyLogging
import scala._
import org.scassandra.server.cqlmessages.types.{CqlMap, CqlList, CqlSet, ColumnType}

case class Rows(keyspaceName: String, tableName: String, stream : Byte, columnTypes : Map[String, ColumnType[_]], rows : List[Row] = List[Row]())(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Rows, stream, protocolVersion.serverCode) with LazyLogging {

  import CqlProtocolHelper._

  override def serialize() : ByteString = {
    val bodyBuilder = ByteString.newBuilder

    bodyBuilder.putInt(resultKind)
    bodyBuilder.putInt(1) // flags
    bodyBuilder.putInt(columnTypes.size) // col count

    bodyBuilder.putBytes(CqlProtocolHelper.serializeString(keyspaceName).toArray)
    bodyBuilder.putBytes(CqlProtocolHelper.serializeString(tableName).toArray)

    // column specs
    val orderedColTypes: List[(String, ColumnType[_])] = columnTypes.toList
    orderedColTypes.foreach( {case (colName, colType) => {
      ResultHelper.serialiseTypeInfomration(colName, colType, bodyBuilder)
    }})

    bodyBuilder.putInt(rows.length)

    rows.foreach(row => {
      orderedColTypes.foreach({
        case (colName, colType) =>
          if (row.columns.get(colName).isDefined) {
            val value = row.columns.get(colName).get
            bodyBuilder.putBytes(colType.writeValueWithLength(value))
          } else {
            // serialise null value
            bodyBuilder.putBytes(NullValue)
          }
      })
    })

    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(bodyBuilder.length)
    bs.putBytes(bodyBuilder.result().toArray)
    bs.result()
  }
}

case class Row(columns : Map[String, Any])

