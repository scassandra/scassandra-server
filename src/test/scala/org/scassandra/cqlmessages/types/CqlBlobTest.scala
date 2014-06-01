package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlBlobTest extends FunSuite with Matchers {

  test("Serialisation of CqlBlog") {
    CqlBlob.writeValue("0x01") should equal(Array(0, 0, 0, 1, 1))
    CqlBlob.writeValue("1235") should equal(Array(0, 0, 0, 2, 18, 53))

    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlBlob.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
