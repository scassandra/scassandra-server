package uk.co.scassandra.server

import akka.util.ByteString
import com.batey.narinc.client.cqlmessages.{HeaderConsts, OpCodes}
import com.batey.narinc.client.cqlmessages.response.ResponseHeader

object MessageHelper {
  def dropHeaderAndLength(bytes: Array[Byte]) : Array[Byte] = {
    bytes drop 8 // drops the header and length
  }
  


  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def createQueryMessage(queryString : String, stream : Byte = ResponseHeader.DefaultStreamId) : List[Byte] = {
    val bodyLength = serializeInt(queryString.size + 4 + 2 + 1)
    val header = List[Byte](0x02, 0x00, stream, OpCodes.Query) :::
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

    val bytes : List[Byte] = List[Byte](HeaderConsts.ClientProtocolVersion, 0x0, 0x0, OpCodes.Startup) :::
      List[Byte](0x0, 0x0, 0x0, messageBody.length.toByte) :::
      messageBody

    bytes
  }

  def createRegisterMessage() : List[Byte] = {
    val header = List[Byte](
      HeaderConsts.ClientProtocolVersion,
      0x0,
      0x0,
      OpCodes.Register
    )
    // TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE
    val registerBody = createRegisterMessageBody()

    header ::: serializeInt(registerBody.length) ::: registerBody
  }

  def createRegisterMessageBody(event : String = "TOPOLOGY_CHANGE") : List[Byte] = {
    val numberOfOptions = serializeShort(1)

    val singleOption = serializeString(event)

    numberOfOptions ::: singleOption
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
