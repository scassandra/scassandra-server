package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlTimeUUIDTest extends FunSuite with Matchers {

  test("Serialisation of CqlTimeUUID") {
    CqlTimeUUID.writeValue("59ad61d0-c540-11e2-881e-b9e6057626c4") should equal(Array(0, 0, 0, 16, 89, -83, 97, -48, -59, 64, 17, -30, -120, 30, -71, -26, 5, 118, 38, -60))

    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue("123")
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlTimeUUID.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
