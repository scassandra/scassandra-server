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
package org.scassandra.server.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteString
import org.scassandra.server.cqlmessages.{VersionTwo, ProtocolVersion}
import org.scassandra.server.cqlmessages.VersionTwo

class ResponseDeserializerTest extends FunSuite with Matchers {
  implicit val protocolVersion = VersionTwo

  test("Test rows msg from real Cassandra") {
    val msg = ByteString(-126, 0, 0, 8,
      0, 0, 0, 81, // length
      0, 0, 0, 2,  // Result type
      0, 0, 0, 1,  // Flags
      0, 0, 0, 2,  // Col count
      0, 6, // keyspace name length
      112, 101, 111, 112, 108, 101,
      0, 6, // table name length
      112, 101, 111, 112, 108, 101,
      0, 2, // col name length
      105, 100, // col name - id
      0, 15, // col type - timeuuid
      0, 10, // col name length
      102, 105, 114, 115, 116, 95, 110, 97, 109, 101, // col name - first_name
      0, 13, // col type - varchar
      0, 0, 0, 1, // row count
      0, 0, 0, 16, // legnth of uuid - id
      -96, 109, 9, 0, 0, 84, 17, -29, -127, -91, 103, 46, -8, -113, 21, -99,
      0, 0, 0, 5, // length of string
      99, 104, 114, 105, 115) // chris
    val response: Response = ResponseDeserializer.deserialize(msg)

    response.isInstanceOf[Rows] should equal(true)
  }

  test("Body should contain the header + body of the correct length - empty message") {
    val emptyMsg = ByteString()

    intercept[IllegalArgumentException] {
      ResponseDeserializer.deserialize(emptyMsg)
    }
  }

  test("Body should contain the header + body of the correct length - header without length") {
    val headerWithoutLength = ByteString(-126, 0, 0, 8)

    intercept[IllegalArgumentException] {
      ResponseDeserializer.deserialize(headerWithoutLength)
    }
  }

  test("Body should contain the header + body of the correct length - body smaller than length") {
    val bodyShorterThanLength = ByteString(-126, 0, 0, 8, 0, 0, 0, 4, 0, 0)

    intercept[IllegalArgumentException] {
      ResponseDeserializer.deserialize(bodyShorterThanLength)
    }
  }

  test("SetKeyspace message from real Cassandra") {
    val setKeyspaceMsgFromCassandra = ByteString(-126, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 3, 0, 6, 112, 101, 111, 112, 108, 101)

    val response = ResponseDeserializer.deserialize(setKeyspaceMsgFromCassandra)

    response.isInstanceOf[SetKeyspace] should equal(true)
  }

  test("Void message de-serialize") {
    val streamId : Byte = 1
    val result = ResponseDeserializer.deserialize(VoidResult(streamId).serialize())

    result.isInstanceOf[VoidResult]
  }
}
