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

import org.scassandra.server.cqlmessages.response._
import org.scassandra.server.cqlmessages.response.UnavailableException
import org.scassandra.server.cqlmessages.response.QueryBeforeReadyMessage
import org.scassandra.server.cqlmessages.response.ReadRequestTimeout
import org.scassandra.server.cqlmessages.response.WriteRequestTimeout
import org.scassandra.server.cqlmessages.response.VoidResult
import org.scassandra.server.cqlmessages.response.Rows
import org.scassandra.server.cqlmessages.response.Ready
import org.scassandra.server.priming.query.Prime
import org.scassandra.server.cqlmessages.response.Row
import org.scassandra.server.cqlmessages.response.SetKeyspace
import akka.util.ByteString
import org.scassandra.server.cqlmessages.request.{ExecuteRequest}
import org.scassandra.server.cqlmessages.types.ColumnType

object VersionTwoMessageFactory extends AbstractMessageFactory {

  val protocolVersion = ProtocolVersion.ServerProtocolVersionTwo
  implicit val protocolVersionImp = VersionTwo

  def createPreparedResult(stream: Byte, id : Int, variableTypes: List[ColumnType[_]]) = {
    PreparedResultV2(stream, id, "keyspace", "table", variableTypes)
  }

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest = {
    ExecuteRequest.versionTwoWithoutTypes(stream, byteString)
  }

  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest = {
    ExecuteRequest.versionTwoWithTypes(stream, byteString, variableTypes)
  }
}
