package org.scassandra.cqlmessages.response

import akka.util.ByteString
import org.scassandra.cqlmessages.{OpCodes, ProtocolVersions, CqlMessage, Header}

object ResponseHeader {
  val VersionByte = ProtocolVersions.ServerProtocolVersionTwo
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId : Byte = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)
}

abstract class Response(header : Header) extends CqlMessage(header)

case class Ready(stream : Byte = ResponseHeader.DefaultStreamId) extends Response(new Header(ProtocolVersions.ServerProtocolVersionTwo, opCode = OpCodes.Ready, streamId = stream)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  val MessageLength = 0

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(MessageLength)
    bs.result()
  }
}