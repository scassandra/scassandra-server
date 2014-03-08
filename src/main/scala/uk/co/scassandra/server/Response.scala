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

object ResultTypes {
  val SetKeyspace : Int = 0x0003
  val VoidResult : Int = 1
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

case class VoidResult(stream: Byte = ResponseHeader.DefaultStreamId) extends Response(new Header(OpCodes.Result, stream)) {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  val Length = 4

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(Length)
    bs.putInt(ResultTypes.VoidResult)
    bs.result()
  }
}

case class Ready(stream : Byte = ResponseHeader.DefaultStreamId) extends Response(new Header(OpCodes.Ready, stream)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(0)
    bs.result()
  }
}

class Error(val errorCode : Int, val errorMessage : String, stream: Byte) extends Response(new Header(OpCodes.Error, stream)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())

    bs.putInt(4 + 2 + errorMessage.length)

    bs.putInt(errorCode)
    bs.putShort(errorMessage.length)
    bs.putBytes(errorMessage.getBytes())

    bs.result()
  }
}

case class QueryBeforeReadyMessage(stream : Byte = ResponseHeader.DefaultStreamId) extends Error(0xA, "Query sent before StartUp message", stream)

case class SetKeyspace(keyspaceName : String, stream : Byte = ResponseHeader.DefaultStreamId) extends Response(new Header(OpCodes.Result, stream)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())

    bs.putInt(4 + 2 + keyspaceName.length)

    bs.putInt(ResultTypes.SetKeyspace)
    bs.putShort(keyspaceName.length)
    bs.putBytes(keyspaceName.getBytes)

    bs.result()
  }
}