package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}

class CqlMapTest extends FunSuite with Matchers {
  test("Serialisation of CqlMap - Varchar") {
    val underTest = CqlMap(CqlVarchar, CqlVarchar)

    underTest.writeValue(Map("one" -> "two", "three" -> "four")) should equal(Array[Byte](
      0, 0, 0, 25, // number of bytes
      0, 2,         // number of elements in the map
      0, 3,   111, 110, 101,  // one
      0, 3,   116, 119, 111,  // two
      0, 5,   116, 104, 114, 101, 101, // three
      0, 4,   102, 111, 117, 114 // four

    ))

    intercept[IllegalArgumentException] {
      underTest.writeValue(List())
    }

    intercept[IllegalArgumentException] {
      underTest.writeValue("0x01")
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue("1235")
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
  }

}
