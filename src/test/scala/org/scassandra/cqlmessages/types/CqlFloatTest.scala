package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlFloatTest extends FunSuite with Matchers {
  test("Serialisation of CqlFloat") {
    CqlFloat.writeValue(BigDecimal("123")) should equal(Array(0, 0, 0, 4, 66, -10, 0, 0))
    CqlFloat.writeValue("123") should equal(Array(0, 0, 0, 4, 66, -10, 0, 0))
    CqlFloat.writeValue(BigDecimal("123.67")) should equal(Array(0, 0, 0, 4, 66, -9, 87, 10))

    intercept[IllegalArgumentException] {
      CqlFloat.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlFloat.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }

}
