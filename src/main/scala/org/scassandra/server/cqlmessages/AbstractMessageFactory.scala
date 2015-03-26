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
import org.scassandra.server.priming.{WriteRequestTimeoutResult, ReadRequestTimeoutResult}
import org.scassandra.server.priming.query.Prime
import org.scassandra.server.cqlmessages.response.Row
import org.scassandra.server.cqlmessages.response.SetKeyspace
import org.scassandra.server.cqlmessages.types.ColumnType

/**
 * Contains all the common messages between version 1
 * and version 2 of the protocol.
 */
abstract class AbstractMessageFactory extends CqlMessageFactory {

  val protocolVersion : Byte
  implicit val protocolVersionImp : ProtocolVersion

  override def createReadyMessage(stream: Byte): Ready = {
    Ready(stream)
  }

  def createQueryBeforeErrorMessage(): QueryBeforeReadyMessage = {
    QueryBeforeReadyMessage(ResponseHeader.DefaultStreamId)
  }

  def createSetKeyspaceMessage(keyspaceName: String, stream: Byte): SetKeyspace = {
    SetKeyspace(keyspaceName, stream)
  }

  def createRowsMessage(prime: Prime, stream: Byte): Rows = {
    Rows(prime.keyspace, prime.table, stream, prime.columnTypes, prime.rows.map(row => Row(row)))
  }
  def createEmptyRowsMessage(stream: Byte): Rows = {
    Rows("","",stream,Map[String, ColumnType[_]](), List())
  }

  override def createReadTimeoutMessage(stream: Byte, consistency: Consistency, readRequestTimeoutResult: ReadRequestTimeoutResult): ReadRequestTimeout = {
    ReadRequestTimeout(stream, consistency, readRequestTimeoutResult)
  }

  override def createWriteTimeoutMessage(stream: Byte, consistency: Consistency, writeRequestTimeoutResult: WriteRequestTimeoutResult): WriteRequestTimeout = {
    WriteRequestTimeout(stream, consistency, writeRequestTimeoutResult)
  }

  override def createUnavailableMessage(stream: Byte, consistency: Consistency): UnavailableException = {
    UnavailableException(stream, consistency)
  }

  def createVoidMessage(stream: Byte): VoidResult = {
    VoidResult(stream)
  }
}
