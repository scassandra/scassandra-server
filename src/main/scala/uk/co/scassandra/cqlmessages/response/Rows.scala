package org.scassandra.cqlmessages.response

import org.scassandra.cqlmessages._
import akka.util.ByteString
import java.math.BigDecimal
import java.util.UUID
import java.net.InetAddress

case class Rows(keyspaceName: String, tableName: String, stream : Byte, columnTypes : Map[String, ColumnType], rows : List[Row] = List[Row]())(implicit protocolVersion: ProtocolVersion) extends Result(ResultKinds.Rows, stream, protocolVersion.serverCode) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())

    val bodyBuilder = ByteString.newBuilder

    bodyBuilder.putInt(resultKind)
    bodyBuilder.putInt(1) // flags
    bodyBuilder.putInt(columnTypes.size) // col count

    bodyBuilder.putBytes(CqlProtocolHelper.serializeString(keyspaceName).toArray)
    bodyBuilder.putBytes(CqlProtocolHelper.serializeString(tableName).toArray)

    // column specs
    columnTypes.foreach( {case (colName, colType) => {
      bodyBuilder.putBytes(CqlProtocolHelper.serializeString(colName).toArray)
      bodyBuilder.putShort(colType.code)
    }})

    bodyBuilder.putInt(rows.length)

    rows.foreach(row => {
      row.columns.foreach({ case (name, value) => {
        columnTypes(name) match {
          case CqlVarchar | CqlAscii | CqlText => bodyBuilder.putBytes(CqlProtocolHelper.serializeLongString(value.toString))
          case CqlInt => {
            bodyBuilder.putInt(4)
            val valueAsInt = if (value.isInstanceOf[String]) value.toString.toInt else value.asInstanceOf[Int]
            bodyBuilder.putInt(valueAsInt)
          }
          case CqlBoolean => bodyBuilder.putBytes(CqlProtocolHelper.serializeBooleanValue(value.toString.toBoolean))
          case CqlBigint | CqlCounter => bodyBuilder.putBytes(CqlProtocolHelper.serializeBigIntValue(value.toString.toLong))
          case CqlBlob => {
            val array = hex2Bytes(value.toString)
            bodyBuilder.putInt(array.length)
            bodyBuilder.putBytes(array)
          }
          case CqlDecimal => bodyBuilder.putBytes(CqlProtocolHelper.serializeDecimalValue(new BigDecimal(value.toString)))
          case CqlDouble => bodyBuilder.putBytes(CqlProtocolHelper.serializeDoubleValue(value.toString.toDouble))
          case CqlFloat => bodyBuilder.putBytes(CqlProtocolHelper.serializeFloatValue(value.toString.toFloat))
          case CqlTimestamp => bodyBuilder.putBytes(CqlProtocolHelper.serializeTimestampValue(value.toString.toLong))
          case CqlUUID | CqlTimeUUID => bodyBuilder.putBytes(CqlProtocolHelper.serializeUUIDValue(UUID.fromString(value.toString)))
          case CqlInet => {
            bodyBuilder.putBytes(CqlProtocolHelper.serializeInetValue(InetAddress.getByName(value.toString)))
          }
          case CqlVarint => bodyBuilder.putBytes(CqlProtocolHelper.serializeVarintValue(BigInt(value.toString)))
        }
      }})
    })

    bs.putInt(bodyBuilder.length)
    bs.putBytes(bodyBuilder.result().toArray)
    bs.result()
  }

  private def hex2Bytes(hex: String): Array[Byte] = {
    (for {i <- 0 to hex.length - 1 by 2 if i > 0 || !hex.startsWith("0x")}
    yield hex.substring(i, i + 2))
      .map(hexValue => Integer.parseInt(hexValue, 16).toByte).toArray
  }

}

case class Row(columns : Map[String, Any])

