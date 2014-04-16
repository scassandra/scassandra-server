package org.scassandra.cqlmessages.response

import org.scalatest._

class ResponseTest extends FunSuite with Matchers {

  test("Serialisation of a ready response") {
    val stream : Byte = 2
    val readyMessage = Ready(stream)
    val bytes = readyMessage.serialize().toList

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      stream, // stream
      0x02, // message type - 2 (Ready)
      0x0, 0x0, 0x0, 0x0 // 4 byte integer - length (number of bytes)
    ))
  }
}
