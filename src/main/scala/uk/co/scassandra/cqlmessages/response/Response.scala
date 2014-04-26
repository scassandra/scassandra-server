package uk.co.scassandra.cqlmessages.response

import akka.util.ByteString
import uk.co.scassandra.cqlmessages._

object ResponseHeader {
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId : Byte = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)
}

abstract class Response(header : Header) extends CqlMessage(header)

case class Ready(stream : Byte = ResponseHeader.DefaultStreamId)(implicit protocolVersion: ProtocolVersion) extends Response(new Header(protocolVersion.serverCode, opCode = OpCodes.Ready, streamId = stream)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  val MessageLength = 0

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(MessageLength)
    bs.result()
  }
}