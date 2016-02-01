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

import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages.request.{QueryRequest, ExecuteRequest}
import org.scassandra.server.cqlmessages.response.{QueryBeforeReadyMessage, ReadRequestTimeout, Ready, Rows, SetKeyspace, UnavailableException, VoidResult, WriteRequestTimeout, _}
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming._
import org.scassandra.server.priming.query.Prime

/*
Will need renaming. Aim to slowly move all protocol parsing through this
trait so that we can isolate the differences between protocols.
 */
trait CqlMessageFactory {

  val protocolVersion: ProtocolVersion
  def createReadyMessage(stream : Byte) : Ready
  def createQueryBeforeErrorMessage() : QueryBeforeReadyMessage
  def createSetKeyspaceMessage(keyspaceName: String, stream: Byte): SetKeyspace
  def createRowsMessage(prime: Prime, stream: Byte): Rows
  def createEmptyRowsMessage(stream: Byte): Rows
  def createReadTimeoutMessage(stream: Byte, consistency: Consistency, readRequestTimeoutResult: ReadRequestTimeoutResult): ReadRequestTimeout
  def createWriteTimeoutMessage(stream: Byte, consistency: Consistency, writeRequestTimeoutResult: WriteRequestTimeoutResult): WriteRequestTimeout
  def createUnavailableMessage(stream: Byte, consistency: Consistency, unavailableResult: UnavailableResult): UnavailableException
  def createServerErrorMessage(stream: Byte, serverErrorResult: ServerErrorResult): ServerError
  def createProtocolErrorMessage(stream: Byte, protocolErrorResult: ProtocolErrorResult): ProtocolError
  def createBadCredentialsMessage(stream: Byte, badCredentialsResult: BadCredentialsResult): BadCredentials
  def createOverloadedMessage(stream: Byte, overloadedResult: OverloadedResult): Overloaded
  def createIsBootstrappingMessage(stream: Byte, isBootstrappingResult: IsBootstrappingResult): IsBootstrapping
  def createTruncateErrorMessage(stream: Byte, truncateErrorResult: TruncateErrorResult): TruncateError
  def createSyntaxErrorMessage(stream: Byte, syntaxErrorResult: SyntaxErrorResult): SyntaxError
  def createUnauthorizedMessage(stream: Byte, unauthorizedResult: UnauthorizedResult): Unauthorized
  def createInvalidMessage(stream: Byte, invalidResult: InvalidResult): Invalid
  def createConfigErrorMessage(stream: Byte, configErrorResult: ConfigErrorResult): ConfigError
  def createAlreadyExistsMessage(stream: Byte, alreadyExistsResult: AlreadyExistsResult): AlreadyExists
  def createdUnpreparedMessage(stream: Byte, unpreparedResult: UnpreparedResult): Unprepared
  def createVoidMessage(stream: Byte): VoidResult
  def createPreparedResult(stream: Byte, id: Int, variableTypes: List[ColumnType[_]], columns: Map[String, ColumnType[_]]): Result
  def createSupportedMessage(stream: Byte): Supported

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest
  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest

  def parseQueryRequest(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): QueryRequest

  def parseBatchQuery(byteString: ByteIterator): String
  def readVariables(iterator: ByteIterator): List[Array[Byte]]

  def createErrorMessage(result: ErrorResult, stream: Byte, consistency: Consistency): Response = result match {
    case result: ReadRequestTimeoutResult =>
      createReadTimeoutMessage(stream, consistency, result)
    case result: UnavailableResult =>
      createUnavailableMessage(stream, consistency, result)
    case result: WriteRequestTimeoutResult =>
      createWriteTimeoutMessage(stream, consistency, result)
    case result: ServerErrorResult =>
      createServerErrorMessage(stream, result)
    case result: ProtocolErrorResult =>
      createProtocolErrorMessage(stream, result)
    case result: BadCredentialsResult =>
      createBadCredentialsMessage(stream, result)
    case result: OverloadedResult =>
      createOverloadedMessage(stream, result)
    case result: IsBootstrappingResult =>
      createIsBootstrappingMessage(stream, result)
    case result: TruncateErrorResult =>
      createTruncateErrorMessage(stream, result)
    case result: SyntaxErrorResult =>
      createSyntaxErrorMessage(stream, result)
    case result: UnauthorizedResult =>
      createUnauthorizedMessage(stream, result)
    case result: InvalidResult =>
      createInvalidMessage(stream, result)
    case result: ConfigErrorResult =>
      createConfigErrorMessage(stream, result)
    case result: AlreadyExistsResult =>
      createAlreadyExistsMessage(stream, result)
    case result: UnpreparedResult =>
      createdUnpreparedMessage(stream, result)
  }
}
