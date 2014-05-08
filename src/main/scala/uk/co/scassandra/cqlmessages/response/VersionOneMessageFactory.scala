package uk.co.scassandra.cqlmessages.response

import uk.co.scassandra.cqlmessages.{ColumnType, VersionOne, ProtocolVersion}
import uk.co.scassandra.priming.query.Prime

object VersionOneMessageFactory extends CqlMessageFactory {

  val protocolVersion = ProtocolVersion.ServerProtocolVersionOne
  implicit val protocolVersionImp = VersionOne

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

  def createPreparedResult(stream: Byte, id : Int, columnTypes: Map[String, ColumnType]): PreparedResultV1 = {
    PreparedResultV1(stream, id, "keyspace", "table", columnTypes)
  }
}
