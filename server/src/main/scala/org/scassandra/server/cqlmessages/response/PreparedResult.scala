/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
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

import org.scassandra.server.cqlmessages.CqlProtocolHelper._
import org.scassandra.server.cqlmessages._
import akka.util.ByteString
import org.scassandra.server.cqlmessages.types.ColumnType

case class PreparedResultV1(stream: Byte, preparedStatementId: Int, keyspaceName: String, tableName: String, variableTypes : List[ColumnType[_]])(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Prepared, stream, protocolVersion.serverCode) {

  import CqlProtocolHelper._

  def serialize(): ByteString = {

    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(ResultKinds.Prepared)

    bodyBs.putShort(4)
    bodyBs.putInt(preparedStatementId)

    bodyBs.putInt(1) // flags
    bodyBs.putInt(variableTypes.size) // col count

    bodyBs.putBytes(serializeString(keyspaceName).toArray)
    bodyBs.putBytes(serializeString(tableName).toArray)

    // column specs
    for (i <- 0 until variableTypes.length) {
      ResultHelper.serialiseTypeInfomration(i.toString, variableTypes(i), bodyBs)
    }

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class PreparedResultV2(stream: Byte, preparedStatementId: Int, keyspaceName: String, tableName: String, variableTypes: List[ColumnType[_]], columns: Map[String, ColumnType[_]])(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Prepared, stream, protocolVersion.serverCode) {

  import CqlProtocolHelper._

  def serialize(): ByteString = {

    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(ResultKinds.Prepared)

    // this is short bytes
    bodyBs.putShort(4)
    bodyBs.putInt(preparedStatementId)

    bodyBs.putInt(1) // flags
    bodyBs.putInt(variableTypes.size + columns.size) // col count

    bodyBs.putBytes(serializeString(keyspaceName).toArray)
    bodyBs.putBytes(serializeString(tableName).toArray)

    // column specs
    for (i <- 0 until variableTypes.length) {
      ResultHelper.serialiseTypeInfomration(i.toString, variableTypes(i), bodyBs)
    }

    columns.foreach { case (columnName: String, columnType: ColumnType[_]) =>
      ResultHelper.serialiseTypeInfomration(columnName, columnType, bodyBs)
    }

    // second meta data - 3 indicates it does not exist
    bodyBs.putInt(RowsFlags.HasNoMetaData)
    // 0 columns
    bodyBs.putInt(0)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}
