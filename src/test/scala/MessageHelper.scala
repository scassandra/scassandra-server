import akka.util.ByteString

object MessageHelper {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def createQueryMessage(queryString : String) : List[Byte] = {
    val bodyLength = serializeInt(queryString.size + 4 + 2 + 1)
    val header = List[Byte](0x02, 0x00, 0x00, OpCodes.Query) :::
      bodyLength

    val body : List[Byte] =
      serializeLongString(queryString) :::
        serializeShort(0x001) ::: // consistency
        List[Byte](0x00) ::: // query flags
        List[Byte]()

    header ::: body
  }

  def createStartupMessage() : List[Byte] = {
    val messageBody = List[Byte](0x0, 0x1 , // number of start up options
    0x0, "CQL_VERSION".length.toByte)  :::
    "CQL_VERSION".getBytes.toList :::
      List[Byte](0x0, "3.0.0".length.toByte) :::
      "3.0.0".getBytes.toList

    val bytes : List[Byte] = List[Byte](HeaderConsts.ProtocolVersion, 0x0, 0x0, OpCodes.Startup) :::
      List[Byte](0x0, 0x0, 0x0, messageBody.length.toByte) :::
      messageBody

    bytes
  }

  private def serializeLongString(string: String): List[Byte] = {
    serializeInt(string.length) :::
      serializeString(string)
  }
  private def serializeInt(int: Int): List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(int)
    frameBuilder.result().toList
  }
  private def serializeString(string: String): List[Byte] = {
    string.getBytes.toList
  }
  private def serializeShort(short : Short) : List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(short)
    frameBuilder.result().toList
  }

}
