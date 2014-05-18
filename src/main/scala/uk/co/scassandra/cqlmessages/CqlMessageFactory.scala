package uk.co.scassandra.cqlmessages

import uk.co.scassandra.cqlmessages.response._
import uk.co.scassandra.cqlmessages.response.UnavailableException
import uk.co.scassandra.cqlmessages.response.QueryBeforeReadyMessage
import uk.co.scassandra.cqlmessages.response.ReadRequestTimeout
import uk.co.scassandra.cqlmessages.response.WriteRequestTimeout
import uk.co.scassandra.cqlmessages.response.VoidResult
import uk.co.scassandra.cqlmessages.response.Rows
import uk.co.scassandra.cqlmessages.response.Ready
import uk.co.scassandra.priming.query.Prime
import uk.co.scassandra.cqlmessages.response.SetKeyspace
import akka.util.ByteString
import uk.co.scassandra.cqlmessages.request.{ExecuteRequest, Request}

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
