/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.cqlmessages

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
