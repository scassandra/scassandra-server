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

  def serializeLongString(string: String): List[Byte] = {
    serializeInt(string.length) :::
      serializeString(string)
  }
  def serializeInt(int: Int): List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(int)
    frameBuilder.result().toList
  }
  def serializeString(string: String): List[Byte] = {
    string.getBytes.toList
  }
  def serializeShort(short : Short) : List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(short)
    frameBuilder.result().toList
  }

}
