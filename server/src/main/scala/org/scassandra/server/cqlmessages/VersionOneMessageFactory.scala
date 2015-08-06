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
package org.scassandra.server.cqlmessages

import org.scassandra.server.cqlmessages.response.PreparedResultV1
import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages.request.{ExecuteRequest}
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming.BatchQuery

object VersionOneMessageFactory extends AbstractMessageFactory {

  val protocolVersion = ProtocolVersion.ServerProtocolVersionOne
  implicit val protocolVersionImp = VersionOne

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
}
