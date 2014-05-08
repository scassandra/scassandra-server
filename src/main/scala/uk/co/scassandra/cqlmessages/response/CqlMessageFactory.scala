package uk.co.scassandra.cqlmessages.response

import uk.co.scassandra.cqlmessages.ColumnType
import uk.co.scassandra.priming.query.Prime

trait CqlMessageFactory {
  def createReadyMessage(stream : Byte) : Ready
  def createQueryBeforeErrorMessage() : QueryBeforeReadyMessage
  def createSetKeyspaceMessage(keyspaceName: String, stream: Byte): SetKeyspace
  def createRowsMessage(prime: Prime, stream: Byte): Rows
  def createReadTimeoutMessage(stream: Byte): ReadRequestTimeout
  def createWriteTimeoutMessage(stream: Byte): WriteRequestTimeout
  def createUnavailableMessage(stream: Byte): UnavailableException
  def createVoidMessage(stream: Byte): VoidResult
  def createPreparedResult(stream: Byte, id: Int, columnTypes: Map[String, ColumnType]): Result
}
