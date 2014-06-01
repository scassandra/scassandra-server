package org.scassandra.cqlmessages.types

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteString

class CqlAsciiTest extends FunSuite with Matchers {

  test("Serialisation of CqlAscii") {
    CqlAscii.writeValue("hello") should equal(Array[Byte](0, 0, 0, 5, 104, 101, 108, 108, 111))
    CqlAscii.writeValueInCollection("hello") should equal(Array[Byte](0, 5, 104, 101, 108, 108, 111))
    CqlAscii.writeValue("") should equal(Array[Byte](0, 0, 0, 0))
    CqlAscii.writeValueInCollection("") should equal(Array[Byte](0, 0))
    CqlAscii.writeValue(BigDecimal("123.67")) should equal(Array[Byte](0, 0, 0, 6, 49, 50, 51, 46, 54, 55))
    CqlAscii.writeValueInCollection(BigDecimal("123.67")) should equal(Array[Byte](0, 6, 49, 50, 51, 46, 54, 55))
    CqlAscii.writeValue(true) should equal(Array[Byte](0, 0, 0, 4, 116, 114, 117, 101))
    CqlAscii.writeValueInCollection(true) should equal(Array[Byte](0, 4, 116, 114, 117, 101))

    intercept[IllegalArgumentException] {
      CqlAscii.writeValue(List())
      CqlAscii.writeValueInCollection(List())
    }
    intercept[IllegalArgumentException] {
      CqlAscii.writeValue(Map())
      CqlAscii.writeValueInCollection(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlAscii.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
