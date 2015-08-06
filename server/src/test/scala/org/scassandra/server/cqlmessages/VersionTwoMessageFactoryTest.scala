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
