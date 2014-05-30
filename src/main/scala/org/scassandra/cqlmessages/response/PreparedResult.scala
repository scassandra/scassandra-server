package org.scassandra.cqlmessages.response

import org.scassandra.cqlmessages._
import akka.util.ByteString

case class PreparedResultV1(stream: Byte, preparedStatementId: Int, keyspaceName: String, tableName: String, variableTypes : List[ColumnType[_]])(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Prepared, stream, protocolVersion.serverCode) {

  import CqlProtocolHelper._

  def serialize(): ByteString = {
    val bs = ByteString.newBuilder

    bs.putBytes(header.serialize())

    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(ResultKinds.Prepared)

    bodyBs.putShort(4)
    bodyBs.putInt(preparedStatementId)

    bodyBs.putInt(1) // flags
    bodyBs.putInt(variableTypes.size) // col count

    bodyBs.putBytes(CqlProtocolHelper.serializeString(keyspaceName).toArray)
    bodyBs.putBytes(CqlProtocolHelper.serializeString(tableName).toArray)

    // column specs
    for (i <- 0 until variableTypes.length) {
      ResultHelper.serialiseTypeInfomration(i.toString, variableTypes(i), bodyBs)
    }

    val bodyResult: ByteString = bodyBs.result()
    bs.putInt(bodyResult.size)
    bs.putBytes(bodyResult.toArray)
    bs.result()
  }
}

case class PreparedResultV2(stream: Byte, preparedStatementId: Int, keyspaceName: String, tableName: String, variableTypes: List[ColumnType[_]])(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Prepared, stream, protocolVersion.serverCode) {

  import CqlProtocolHelper._

  def serialize(): ByteString = {
    val bs = ByteString.newBuilder

    bs.putBytes(header.serialize())

    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(ResultKinds.Prepared)

    // this is short bytes
    bodyBs.putShort(4)
    bodyBs.putInt(preparedStatementId)

    bodyBs.putInt(1) // flags
    bodyBs.putInt(variableTypes.size) // col count

    bodyBs.putBytes(CqlProtocolHelper.serializeString(keyspaceName).toArray)
    bodyBs.putBytes(CqlProtocolHelper.serializeString(tableName).toArray)

    // column specs
    for (i <- 0 until variableTypes.length) {
      ResultHelper.serialiseTypeInfomration(i.toString, variableTypes(i), bodyBs)
    }

    // second meta data - 3 indicates it does not exist
    bodyBs.putInt(RowsFlags.HasNoMetaData)
    // 0 columns
    bodyBs.putInt(0)

    val bodyResult: ByteString = bodyBs.result()
    bs.putInt(bodyResult.size)
    bs.putBytes(bodyResult.toArray)
    bs.result()
  }
}