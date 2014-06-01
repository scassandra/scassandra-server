package org.scassandra.cqlmessages.types

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteString

class CqlTimestampTest extends FunSuite with Matchers {
  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlTimestamp.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
