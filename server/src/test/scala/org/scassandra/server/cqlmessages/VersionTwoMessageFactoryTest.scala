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
package org.scassandra.server.cqlmessages

import akka.util.{ByteIterator, ByteString}
import org.scalatest.{Matchers, FunSpec}
import org.scassandra.server.priming.BatchQuery

class VersionTwoMessageFactoryTest extends FunSpec with Matchers {

  import CqlProtocolHelper._

  describe("Batch query parsing") {
    describe("Regular queries") {
      it("Parse query and kind") {
        val underTest = VersionTwoMessageFactory
        val bytes: Array[Byte] = serializeLongString("some query") ++ serializeShort(0)

        val batchQuery = underTest.parseBatchQuery(ByteString(bytes).iterator)

        batchQuery should equal("some query")
      }

      it("Drop query parameters") {
        val underTest = VersionTwoMessageFactory
        val bytes: Array[Byte] = serializeLongString("some query") ++
          serializeShort(1) ++ serializeLongString("Some paramters")
        val iterator: ByteIterator = ByteString(bytes).iterator

        underTest.parseBatchQuery(iterator)

        iterator.isEmpty should be(true)
      }
    }
  }
}
