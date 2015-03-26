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

import akka.util.ByteString
import org.scassandra.server.cqlmessages.request.ExecuteRequest
import org.scassandra.server.cqlmessages.response.{QueryBeforeReadyMessage, ReadRequestTimeout, Ready, Rows, SetKeyspace, UnavailableException, VoidResult, WriteRequestTimeout, _}
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming.{WriteRequestTimeoutResult, ReadRequestTimeoutResult}
import org.scassandra.server.priming.query.Prime

trait CqlMessageFactory {
  def createReadyMessage(stream : Byte) : Ready
  def createQueryBeforeErrorMessage() : QueryBeforeReadyMessage
  def createSetKeyspaceMessage(keyspaceName: String, stream: Byte): SetKeyspace
  def createRowsMessage(prime: Prime, stream: Byte): Rows
  def createEmptyRowsMessage(stream: Byte): Rows
  def createReadTimeoutMessage(stream: Byte, consistency: Consistency, readRequestTimeoutResult: ReadRequestTimeoutResult): ReadRequestTimeout
  def createWriteTimeoutMessage(stream: Byte, consistency: Consistency, writeRequestTimeoutResult: WriteRequestTimeoutResult): WriteRequestTimeout
  def createUnavailableMessage(stream: Byte, consistency: Consistency): UnavailableException
  def createVoidMessage(stream: Byte): VoidResult
  def createPreparedResult(stream: Byte, id: Int, variableTypes: List[ColumnType[_]]): Result

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest
  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest
}
