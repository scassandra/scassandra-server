package uk.co.scassandra.server

import akka.util.ByteString
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.batey.narinc.client.cqlmessages.{VoidResult, OpCodes}

class ResponseTest extends FunSuite with ShouldMatchers {

  def toNativeProtocolString(string: String): List[Byte] = {
    // [string] = [short] + n bytes
    List[Byte](
      // Note: this assumes that the string length fits in one byte == max 127 chars
      0x0, string.length.toByte
    ) ::: string.getBytes.toList
  }

  def serializeInt(int: Int): List[Byte] = {
    implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

    val builder = ByteString.newBuilder
    builder.putInt(int)
    var result = builder.result().toList
    while (result.length < 4) {
      result = 0x00.toByte :: result
    }
    result
  }

  test("Serialisation of a void result") {
    val stream: Byte = 0x01
    val voidResult = VoidResult(stream)
    val bytes = voidResult.serialize().toList

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      stream, // stream
      0x08, // message type - 8 (Result)
      0x0, 0x0, 0x0, 0x4, // 4 byte integer - length (number of bytes)
      0x0, 0x0, 0x0, 0x1 // 4 byte integer for the type of result, 1 is a void result
    ))
  }

  test("Serialisation of a ready response") {
    val readyMessage = Ready()
    val bytes = readyMessage.serialize().toList

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      0x00, // stream
      0x02, // message type - 2 (uk.co.scassandra.server.uk.co.scassandra.server.Ready)
      0x0, 0x0, 0x0, 0x0 // 4 byte integer - length (number of bytes)
    ))
  }
}
