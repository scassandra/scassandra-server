package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlDoubleTest extends FunSuite with Matchers {

  test("Serialisation of CqlDouble") {
    CqlDouble.writeValue(BigDecimal("123")) should equal(Array(0, 0, 0, 8, 64, 94, -64, 0, 0, 0, 0, 0))
    CqlDouble.writeValue("123") should equal(Array(0, 0, 0, 8, 64, 94, -64, 0, 0, 0, 0, 0))
    CqlDouble.writeValue(BigDecimal("123.67")) should equal(Array(0, 0, 0, 8, 64, 94, -22, -31, 71, -82, 20, 123))

    intercept[IllegalArgumentException] {
      CqlDouble.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlDouble.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }

}
