package uk.co.scassandra.server

import akka.util.ByteString
import com.batey.narinc.client.cqlmessages.{HeaderConsts, OpCodes}

class Response(val header : Header) {
  def serialize() : ByteString = ???
}

object ResponseHeader {
  val VersionByte = 0x82
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId : Byte = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)
}

class Header(val opCode : Int, val streamId : Byte) {
  val flags : Int = ResponseHeader.FlagsNoCompressionByte

  def serialize() : Array[Byte] = {
    val bs = ByteString.newBuilder

    bs.putByte(HeaderConsts.ServerProtocolVersion)
    bs.putByte(flags.toByte)
    bs.putByte(streamId)
    bs.putByte(opCode.toByte)

    bs.result().toArray
  }
}
