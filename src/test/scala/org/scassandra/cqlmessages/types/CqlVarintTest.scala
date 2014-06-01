package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlVarintTest extends FunSuite with Matchers {

  test("Serialisation of CqlVarint") {
    CqlVarint.writeValue(BigDecimal("123000000000")) should equal(Array(0, 0, 0, 5, 28, -93, 95, 14, 0))
    CqlVarint.writeValue("123") should equal(Array(0, 0, 0, 1, 123))

    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlVarint.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
