package org.scassandra.cqlmessages.types

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteString

class CqlBigintTest extends FunSuite with Matchers {

  test("Serialisation of CqlBigInt") {
    CqlBigint.writeValue(BigDecimal("123000000000")) should equal(Array(0, 0, 0, 8, 0, 0, 0, 28, -93, 95, 14, 0))
    CqlBigint.writeValue("123") should equal(Array[Byte](0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue("hello") should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(true) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(false) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(List()) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(Map()) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlBigint.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
