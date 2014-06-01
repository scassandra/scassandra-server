package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlUUIDTest extends FunSuite with Matchers {

  test("Serialisation of CqlUUID") {
    CqlUUID.writeValue("59ad61d0-c540-11e2-881e-b9e6057626c4") should equal(Array(0, 0, 0, 16, 89, -83, 97, -48, -59, 64, 17, -30, -120, 30, -71, -26, 5, 118, 38, -60))

    intercept[IllegalArgumentException] {
      CqlUUID.writeValue("123")
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlUUID.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
