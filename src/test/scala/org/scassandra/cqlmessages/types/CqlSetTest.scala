/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.cqlmessages.types

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString

class CqlSetTest extends FunSuite with Matchers {

  test("Serialisation of CqlSet - Varchar") {
    val underTest = CqlSet(CqlVarchar)
    underTest.writeValue(List()) should equal(Array[Byte](0, 0, 0, 2, 0, 0))

    underTest.writeValue(List("one", "two", "three")) should equal(Array[Byte](
      0, 0, 0, 19, // number of bytes
      0, 3, // number of elements in the set
      0, 3, 111, 110, 101, // one
      0, 3, 116, 119, 111, // two
      0, 5, 116, 104, 114, 101, 101 // three

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

  test("Serialisation of CqlSet - Ascii") {
    val underTest = CqlSet(CqlAscii)
    underTest.writeValue(List()) should equal(Array[Byte](0, 0, 0, 2, 0, 0))

    underTest.writeValue(List("one", "two", "three")) should equal(Array[Byte](
      0, 0, 0, 19, // number of bytes
      0, 3, // number of elements in the set
      0, 3, 111, 110, 101, // one
      0, 3, 116, 119, 111, // two
      0, 5, 116, 104, 114, 101, 101 // three

    ))
  }

  test("Serialisation of CqlSet - CqlText") {
    val underTest = CqlSet(CqlText)
    underTest.writeValue(List()) should equal(Array[Byte](0, 0, 0, 2, 0, 0))

    underTest.writeValue(List("one", "two", "three")) should equal(Array[Byte](
      0, 0, 0, 19, // number of bytes
      0, 3, // number of elements in the set
      0, 3, 111, 110, 101, // one
      0, 3, 116, 119, 111, // two
      0, 5, 116, 104, 114, 101, 101 // three

    ))
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1, -1, -1, -1))
    val deserialisedValue = CqlSet(CqlVarchar).readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
