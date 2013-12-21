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

class Header(val opCode : Int, val length : Int) {
  val version = ResponseHeader.VersionByte
  val flags : Int = ResponseHeader.FlagsNoCompressionByte
  val streamId : Int = ResponseHeader.DefaultStreamId
}
class Response(val header : Header) {
  def serialize() : ByteString = ???
}

object VoidResult extends Response(new Header(OpCodes.Result, 4)) {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  val Length = 4

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putByte((header.version & 0xFF).toByte)
    bs.putByte(header.flags.toByte)
    bs.putByte(header.streamId.toByte)
    bs.putByte(header.opCode.toByte)
    bs.putInt(Length)
    bs.putInt(ResultTypes.VoidResult)
    bs.result()
  }
}

object Ready extends Response(new Header(OpCodes.Ready, 0)) {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  override def serialize() : ByteString = {
    val bs = ByteString.newBuilder
    bs.putByte((header.version & 0xFF).toByte)
    bs.putByte(header.flags.toByte)
    bs.putByte(header.streamId.toByte)
    bs.putByte(header.opCode.toByte)
    bs.putInt(0)
    bs.result()
  }
}