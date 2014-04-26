package uk.co.scassandra.cqlmessages.response

import akka.util.ByteString
import java.util.UUID
import com.typesafe.scalalogging.slf4j.Logging
import java.nio.ByteOrder
import uk.co.scassandra.cqlmessages._

abstract class Result(val resultKind: Int, val streamId: Byte, protocolVersion : Byte) extends Response(new Header(protocolVersion, OpCodes.Result, streamId))

case class VoidResult(stream : Byte)(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.VoidResult, stream, protocolVersion.serverCode) {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  val Length = 4

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(Length)
    bs.putInt(resultKind)
    bs.result()
  }
}

object Result extends Logging {

  implicit val byteOrder : ByteOrder = ByteOrder.BIG_ENDIAN


  def fromByteString(byteString : ByteString) : Result = {
    // TODO: change to be based off the incoming message
    implicit val impProtocolVersion = VersionTwo
    val iterator = byteString.iterator
    val protocolVersion = iterator.getByte
    val flags = iterator.getByte
    val stream = iterator.getByte
    val opCode = iterator.getByte
    val bodyLength = iterator.getInt
    val resultKind = iterator.getInt

    resultKind match {
      case ResultKinds.SetKeyspace =>
        val keyspaceNameLength = iterator.getShort
        val keyspaceName = ResponseDeserializer.readString(iterator, keyspaceNameLength)
        logger.info(s"Received set keyspace '${keyspaceName}'")
        SetKeyspace(keyspaceName)
      case ResultKinds.VoidResult =>
        VoidResult(stream)
      case ResultKinds.Rows => {
        val rowsFlags = iterator.getInt
        logger.debug(s"Rows flags ${rowsFlags}")
        val columnCount = iterator.getInt
        logger.debug(s"Rows have ${columnCount} columns")

        val keyspaceNameLength = iterator.getShort
        val keyspaceName = ResponseDeserializer.readString(iterator, keyspaceNameLength)
        logger.debug(s"Keyspace name length ${keyspaceNameLength} and name ${keyspaceName}")
        val tableNameLength = iterator.getShort
        val tableName = ResponseDeserializer.readString(iterator, tableNameLength)
        logger.debug(s"Table name length ${tableNameLength} and name ${tableName}")

        val columnNamesAndTypes = (0 until columnCount).map(columnNumber => {
          val colNameLength = iterator.getShort
          val colName = ResponseDeserializer.readString(iterator, colNameLength)
          val columnType = iterator.getShort
          logger.debug(s"Column name ${new String(colName)} with type ${columnType}")
          (colName, columnType)
        }).toList

        val rowCount = iterator.getInt
        val rowData: List[Row] = (0 until rowCount).map(rowNumber => {

          val colValues: List[(String, Any)] = columnNamesAndTypes.map(colNameAndType => {
            val (colName, colType) = colNameAndType
            val colValueLength = iterator.getInt
            logger.debug(s"Dealing with column name ${colName} col type ${colType} with length ${colValueLength}")
            val colValue = colType match {
              case CqlTimeUUID.code => {
                val timeUUid = new UUID(iterator.getLong, iterator.getLong)
                timeUUid.toString
              }
              case CqlBigint.code | CqlCounter.code => {
                iterator.getLong
              }
              case CqlBlob.code => {
                val colValueBytes = new Array[Byte](colValueLength)
                iterator.getBytes(colValueBytes)
                colValueBytes
              }
              case CqlBoolean.code => {
                val booleanByte = iterator.getByte
                (booleanByte == 1)
              }
              case _ => {
                val colValueBytes = new Array[Byte](colValueLength)
                logger.debug(s"Reading ${colValueLength} bytes")
                iterator.getBytes(colValueBytes)
                new String(colValueBytes)
              }
            }
            (colName, colValue)
          })

          logger.debug(s"Column Values ${colValues}")

          new Row(colValues.toMap)
        }).toList
        Rows(keyspaceName, tableName, stream, columnNamesAndTypes.map( col => (col._1,CqlVarchar) ).toMap , rowData)
     }
    }
  }
}

case class SetKeyspace(keyspaceName : String, stream : Byte = ResponseHeader.DefaultStreamId)(implicit protocolVersion: ProtocolVersion) extends Result(resultKind = ResultKinds.SetKeyspace, streamId = stream, protocolVersion.serverCode) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    val serialisedKeyspaceName: Array[Byte] = CqlProtocolHelper.serializeString(keyspaceName).toArray

    bs.putInt(Header.Length + serialisedKeyspaceName.length)

    bs.putInt(ResultKinds.SetKeyspace)
    bs.putBytes(serialisedKeyspaceName)

    bs.result()
  }
}
