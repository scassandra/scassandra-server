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
import uk.co.scassandra.cqlmessages.response.Row
import uk.co.scassandra.cqlmessages.response.SetKeyspace
import uk.co.scassandra.cqlmessages.response.PreparedResultV1
import akka.util.ByteString
import uk.co.scassandra.cqlmessages.request.ExecuteRequest

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

  def createPreparedResult(stream: Byte, id : Int, variableTypes: List[ColumnType[_]]): PreparedResultV1 = {
    PreparedResultV1(stream, id, "keyspace", "table", variableTypes)
  }

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest = {
    ExecuteRequest.versionOneWithoutTypes(stream, byteString)
  }

  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest = {
    ExecuteRequest.versionOneWithTypes(stream, byteString, variableTypes)
  }
}
