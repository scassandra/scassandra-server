object ResponseHeader {
  val VersionByte = 0x82
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)
}

object ResultTypes {
  val SetKeyspace : Int = 0x0003
  val VoidResult : Int = 0x0001
}

class Header(val opCode : Int, val length : Int) {
  val version : Int = ResponseHeader.VersionByte
  val flags : Int = ResponseHeader.FlagsNoCompressionByte
  val streamId : Int = ResponseHeader.DefaultStreamId
}
class Response(val header : Header) {
  def serialize() : List[Int] = ???
}

object VoidResult extends Response(new Header(OpCodes.Result, 4)) {
  val resultType = ResultTypes.VoidResult
  val rest = List(0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x1)

  override def serialize() : List[Int] = {
    header.version :: header.flags :: header.streamId :: header.opCode :: rest
  }
}
