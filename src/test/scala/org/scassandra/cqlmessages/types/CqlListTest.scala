package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlListTest extends FunSuite with Matchers {
  test("Serialisation of CqlList - Varchar") {
    val underTest = CqlList(CqlVarchar)
    underTest.writeValue(List()) should equal(Array[Byte](0,0,0,2,0,0))
    underTest.writeValue(List("one", "two", "three")) should equal(Array[Byte](
      0, 0, 0, 19, // number of bytes
      0, 3,         // number of elements in the set
      0, 3,   111, 110, 101,  // one
      0, 3,   116, 119, 111,  // two
      0, 5,   116, 104, 114, 101, 101 // three

    ))

    intercept[IllegalArgumentException] {
      underTest.writeValue("0x01")
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue("1235") should equal(Array(0, 0, 0, 2, 18, 53))
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlList(CqlVarchar).readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
