package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlCounterTest extends FunSuite with Matchers {
  test("Serialisation of CqlCounter") {
    CqlCounter.writeValue(BigDecimal("123000000000")) should equal(Array(0, 0, 0, 8, 0, 0, 0, 28, -93, 95, 14, 0))
    CqlCounter.writeValue("123") should equal(Array[Byte](0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlCounter.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }

}
