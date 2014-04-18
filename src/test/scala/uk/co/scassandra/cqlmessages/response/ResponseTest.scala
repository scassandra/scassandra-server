package org.scassandra.cqlmessages.response

import org.scalatest._
import org.scassandra.cqlmessages.ProtocolVersions

class ResponseTest extends FunSuite with Matchers {

  test("Serialisation of a ready response") {
    val stream : Byte = 2
    val readyMessage = Ready(ProtocolVersions.ServerProtocolVersionTwo, stream)
    val bytes = readyMessage.serialize().toList

    bytes should equal(List[Byte](
      ProtocolVersions.ServerProtocolVersionTwo, // protocol version
      0x00, // flags
      stream, // stream
      0x02, // message type - 2 (Ready)
      0x0, 0x0, 0x0, 0x0 // 4 byte integer - length (number of bytes)
    ))
  }
}
