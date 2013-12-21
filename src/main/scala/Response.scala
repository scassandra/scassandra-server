import akka.util.ByteString

object ResponseHeader {
  val VersionByte = 0x82
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)
}

object ResultTypes {
  val SetKeyspace : Int = 0x0003
  val VoidResult : Int = 1
}

class Header(val opCode : Int) {
  val version = ResponseHeader.VersionByte
  val flags : Int = ResponseHeader.FlagsNoCompressionByte
  val streamId : Int = ResponseHeader.DefaultStreamId

  def serialize() : Array[Byte] = {
    val bs = ByteString.newBuilder

    bs.putByte((version & 0xFF).toByte)
    bs.putByte(flags.toByte)
    bs.putByte(streamId.toByte)
    bs.putByte(opCode.toByte)

    bs.result().toArray
  }
}
class Response(val header : Header) {
  def serialize() : ByteString = ???
}

object VoidResult extends Response(new Header(OpCodes.Result)) {
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

object Ready extends Response(new Header(OpCodes.Ready)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putBytes(header.serialize())
    bs.putInt(0)
    bs.result()
  }
}

class Error(val errorCode : Int, val errorMessage : String) extends Response(new Header(OpCodes.Error)) {

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

object QueryBeforeReadyMessage extends Error(0xA, "Query sent before StartUp message")