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
package org.scassandra.server.cqlmessages

import org.scassandra.server.cqlmessages.response.PreparedResultV1
import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages.request.{QueryRequest, ExecuteRequest}
import org.scassandra.server.cqlmessages.types.ColumnType

object VersionOneMessageFactory extends AbstractMessageFactory {

  implicit val protocolVersion = VersionOne
  import CqlProtocolHelper._

  def createPreparedResult(stream: Byte, id : Int, variableTypes: List[ColumnType[_]]): PreparedResultV1 = {
    PreparedResultV1(stream, id, "keyspace", "table", variableTypes)
  }

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest = {
    ExecuteRequest.versionOneWithoutTypes(stream, byteString)
  }

  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest = {
    ExecuteRequest.versionOneWithTypes(stream, byteString, variableTypes)
  }

  def parseBatchQuery(byteString: ByteIterator): String  = throw new UnsupportedOperationException("Batches not supported at v1 of the protocol")

  def readVariables(iterator: ByteIterator): List[Array[Byte]] = throw new UnsupportedOperationException("Batches not supported at v1 of the protocol")

  override def parseQueryRequest(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]] = Nil): QueryRequest = {
    val iterator = byteString.iterator
    val query = CqlProtocolHelper.readLongString(iterator)
    val consistency = iterator.getShort
    // Version One of the protocol doesn't support flags or values so do not parse those.
    QueryRequest(stream, query.getOrElse("Failed to parse Query string"), consistency)
  }
}
