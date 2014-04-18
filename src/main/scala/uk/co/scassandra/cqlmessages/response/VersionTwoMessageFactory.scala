package uk.co.scassandra.cqlmessages.response

import org.scassandra.cqlmessages.response._
import org.scassandra.cqlmessages.response.QueryBeforeReadyMessage
import org.scassandra.cqlmessages.response.Rows
import org.scassandra.cqlmessages.response.Ready
import org.scassandra.cqlmessages.response.SetKeyspace
import uk.co.scassandra.priming.Prime
import org.scassandra.cqlmessages.ProtocolVersions

class VersionTwoMessageFactory extends CqlMessageFactory {
  override def createReadyMessage(stream: Byte): Ready = {
    Ready(ProtocolVersions.ServerProtocolVersionTwo, stream)
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
