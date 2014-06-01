package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlInetTest extends FunSuite with Matchers {
  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlInet.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
