package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlBooleanTest extends FunSuite with Matchers {

  test("Serialisation of CqlBoolean") {
    CqlBoolean.writeValue("true") should equal(Array(0, 0, 0, 1, 1))
    CqlBoolean.writeValue("false") should equal(Array(0, 0, 0, 1, 0))
    CqlBoolean.writeValue("TRUE") should equal(Array(0, 0, 0, 1, 1))
    CqlBoolean.writeValue("FALSE") should equal(Array(0, 0, 0, 1, 0))
    CqlBoolean.writeValue(true) should equal(Array(0, 0, 0, 1, 1))
    CqlBoolean.writeValue(false) should equal(Array(0, 0, 0, 1, 0))

    intercept[IllegalArgumentException] {
      CqlBoolean.writeValue("123")
    }
    intercept[IllegalArgumentException] {
      CqlBoolean.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlBoolean.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlBoolean.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlBoolean.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
