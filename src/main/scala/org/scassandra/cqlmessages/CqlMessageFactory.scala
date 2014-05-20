package org.scassandra.cqlmessages

import org.scassandra.cqlmessages.response._
import org.scassandra.cqlmessages.response.UnavailableException
import org.scassandra.cqlmessages.response.QueryBeforeReadyMessage
import org.scassandra.cqlmessages.response.ReadRequestTimeout
import org.scassandra.cqlmessages.response.WriteRequestTimeout
import org.scassandra.cqlmessages.response.VoidResult
import org.scassandra.cqlmessages.response.Rows
import org.scassandra.cqlmessages.response.Ready
import org.scassandra.priming.query.Prime
import org.scassandra.cqlmessages.response.SetKeyspace
import akka.util.ByteString
import org.scassandra.cqlmessages.request.{ExecuteRequest}

trait CqlMessageFactory {
  def createReadyMessage(stream : Byte) : Ready
  def createQueryBeforeErrorMessage() : QueryBeforeReadyMessage
  def createSetKeyspaceMessage(keyspaceName: String, stream: Byte): SetKeyspace
  def createRowsMessage(prime: Prime, stream: Byte): Rows
  def createEmptyRowsMessage(stream: Byte): Rows
  def createReadTimeoutMessage(stream: Byte): ReadRequestTimeout
  def createWriteTimeoutMessage(stream: Byte): WriteRequestTimeout
  def createUnavailableMessage(stream: Byte): UnavailableException
  def createVoidMessage(stream: Byte): VoidResult
  def createPreparedResult(stream: Byte, id: Int, variableTypes: List[ColumnType[_]]): Result

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest
  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest
}
