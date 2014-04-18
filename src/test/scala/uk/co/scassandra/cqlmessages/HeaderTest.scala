package org.scassandra.cqlmessages

import org.scalatest._

class HeaderTest extends FunSuite with Matchers {
  val stream : Byte = 1
  test("A Header should have the native protocol version 2") {
    val header = new Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Startup, streamId = stream)
    header.version should equal(ProtocolVersion.ClientProtocolVersionTwo)
  }

  test("A Header should have blank flags") {
    val header = new Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Startup, streamId = stream)
    header.flags should equal(0x00)
  }

  test("A Header should have stream it was constructed with") {
    val header = new Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Startup, streamId = stream)
    header.streamId should equal(stream)
  }

  test("A Header should have the opcode it was constructed with") {
    val header = new Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Startup, streamId = stream)
    header.opCode should equal(OpCodes.Startup)
  }
}
