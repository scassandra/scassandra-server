package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlIntTest extends FunSuite with Matchers {

  test("Serialisation of CqlInt") {
    CqlInt.writeValue(BigDecimal("123")) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    CqlInt.writeValue("123") should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlInt.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(BigDecimal("12345678910"))
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlInt.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
