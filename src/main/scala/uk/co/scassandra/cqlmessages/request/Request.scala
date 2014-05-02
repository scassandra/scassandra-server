package uk.co.scassandra.cqlmessages.request

import akka.util.ByteString
import uk.co.scassandra.cqlmessages._

// Current has a fixed stream ID as only sent on startup
case object StartupHeader extends Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Startup, streamId = 0)

case class QueryHeader(stream: Byte) extends Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Query, streamId = stream)

abstract class Request(header: Header) extends CqlMessage(header)

object StartupRequest extends Request(StartupHeader) {

  val options = Map("CQL_VERSION" -> "3.0.0")

  override def serialize() = {
    val header = StartupHeader.serialize()

    val body = CqlProtocolHelper.serializeShort(options.size.toShort) ++
      CqlProtocolHelper.serializeString(options.head._1) ++
      CqlProtocolHelper.serializeString(options.head._2)

    ByteString((header ++ CqlProtocolHelper.serializeInt(body.size) ++ body))
  }
}

case class QueryRequest(stream: Byte, query: String, val consistency : Short = 0x0001, val flags : Byte = 0x00) extends Request(QueryHeader(stream)) {
  override def serialize() = {
    val body =  CqlProtocolHelper.serializeLongString(query) ++
      CqlProtocolHelper.serializeShort(consistency) ++
      CqlProtocolHelper.serializeByte(flags)

    ByteString(header.serialize() ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}

case class PrepareRequest(protocolVersion: Byte, stream: Byte, query: String) extends Request(new Header(protocolVersion, OpCodes.Prepare, stream)) {
  def serialize(): ByteString = {
    val headerBytes: Array[Byte] = header.serialize()
    val body: Array[Byte] = CqlProtocolHelper.serializeLongString(query)
    ByteString(headerBytes ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}

case class ExecuteRequest(protocolVersion: Byte, stream: Byte, id: Int, val consistency : Short = 0x0001, val flags : Byte = 0x00) extends Request(new Header(protocolVersion, OpCodes.Execute, stream)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def serialize(): ByteString = {
    val headerBytes: Array[Byte] = header.serialize()
    val bs = ByteString.newBuilder

    bs.putShort(4)
    bs.putInt(id)

    bs.putShort(consistency)
    bs.putByte(flags)


    val body = bs.result()
    ByteString(headerBytes ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}




