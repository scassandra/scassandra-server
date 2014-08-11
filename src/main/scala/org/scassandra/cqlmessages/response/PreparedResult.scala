package org.scassandra.cqlmessages.response

import org.scassandra.cqlmessages.CqlProtocolHelper._
import org.scassandra.cqlmessages._
import akka.util.ByteString
import org.scassandra.cqlmessages.types.ColumnType

case class PreparedResultV1(stream: Byte, preparedStatementId: Int, keyspaceName: String, tableName: String, variableTypes : List[ColumnType[_]])(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Prepared, stream, protocolVersion.serverCode) {

  import CqlProtocolHelper._

  def serialize(): ByteString = {

    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(ResultKinds.Prepared)

    bodyBs.putShort(4)
    bodyBs.putInt(preparedStatementId)

    bodyBs.putInt(1) // flags
    bodyBs.putInt(variableTypes.size) // col count

    bodyBs.putBytes(serializeString(keyspaceName).toArray)
    bodyBs.putBytes(serializeString(tableName).toArray)

    // column specs
    for (i <- 0 until variableTypes.length) {
      ResultHelper.serialiseTypeInfomration(i.toString, variableTypes(i), bodyBs)
    }

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class PreparedResultV2(stream: Byte, preparedStatementId: Int, keyspaceName: String, tableName: String, variableTypes: List[ColumnType[_]])(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Prepared, stream, protocolVersion.serverCode) {

  import CqlProtocolHelper._

  def serialize(): ByteString = {

    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(ResultKinds.Prepared)

    // this is short bytes
    bodyBs.putShort(4)
    bodyBs.putInt(preparedStatementId)

    bodyBs.putInt(1) // flags
    bodyBs.putInt(variableTypes.size) // col count

    bodyBs.putBytes(serializeString(keyspaceName).toArray)
    bodyBs.putBytes(serializeString(tableName).toArray)

    // column specs
    for (i <- 0 until variableTypes.length) {
      ResultHelper.serialiseTypeInfomration(i.toString, variableTypes(i), bodyBs)
    }

    // second meta data - 3 indicates it does not exist
    bodyBs.putInt(RowsFlags.HasNoMetaData)
    // 0 columns
    bodyBs.putInt(0)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}