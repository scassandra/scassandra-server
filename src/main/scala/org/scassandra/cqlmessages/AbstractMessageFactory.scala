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
import org.scassandra.cqlmessages.response.Row
import org.scassandra.cqlmessages.response.SetKeyspace

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

  def createReadTimeoutMessage(stream: Byte): ReadRequestTimeout = {
    ReadRequestTimeout(stream)
  }

  def createWriteTimeoutMessage(stream: Byte): WriteRequestTimeout = {
    WriteRequestTimeout(stream)
  }

  def createUnavailableMessage(stream: Byte): UnavailableException = {
    UnavailableException(stream)
  }

  def createVoidMessage(stream: Byte): VoidResult = {
    VoidResult(stream)
  }
}
