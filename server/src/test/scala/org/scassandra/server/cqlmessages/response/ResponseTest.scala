/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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
package org.scassandra.server.cqlmessages.response

import org.scalatest._
import org.scassandra.server.cqlmessages.VersionTwo

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
