package uk.co.scassandra.cqlmessages.response

import org.scalatest._
import uk.co.scassandra.cqlmessages.{VersionTwo, ProtocolVersion}

class ResponseTest extends FunSuite with Matchers {

  implicit val protocolVersion = VersionTwo

  test("Serialisation of a ready response") {
    val stream : Byte = 2
    val readyMessage = Ready(stream)
    val bytes = readyMessage.serialize().toList

    bytes should equal(List[Byte](
      protocolVersion.serverCode, // protocol version
      0x00, // flags
      stream, // stream
      0x02, // message type - 2 (Ready)
      0x0, 0x0, 0x0, 0x0 // 4 byte integer - length (number of bytes)
    ))
  }
}
