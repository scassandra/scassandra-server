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
package org.scassandra.server.cqlmessages.types

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteString
import org.scassandra.server.cqlmessages.VersionTwo

class CqlAsciiTest extends FunSuite with Matchers {

  test("Serialisation of CqlAscii") {
    CqlAscii.writeValue("hello") should equal(Array[Byte](0, 0, 0, 5, 104, 101, 108, 108, 111))
    CqlAscii.writeValueInCollection("hello") should equal(Array[Byte](0, 5, 104, 101, 108, 108, 111))
    CqlAscii.writeValue("") should equal(Array[Byte](0, 0, 0, 0))
    CqlAscii.writeValueInCollection("") should equal(Array[Byte](0, 0))
    CqlAscii.writeValue(BigDecimal("123.67")) should equal(Array[Byte](0, 0, 0, 6, 49, 50, 51, 46, 54, 55))
    CqlAscii.writeValueInCollection(BigDecimal("123.67")) should equal(Array[Byte](0, 6, 49, 50, 51, 46, 54, 55))
    CqlAscii.writeValue(true) should equal(Array[Byte](0, 0, 0, 4, 116, 114, 117, 101))
    CqlAscii.writeValueInCollection(true) should equal(Array[Byte](0, 4, 116, 114, 117, 101))

    intercept[IllegalArgumentException] {
      CqlAscii.writeValue(List())
      CqlAscii.writeValueInCollection(List())
    }
    intercept[IllegalArgumentException] {
      CqlAscii.writeValue(Map())
      CqlAscii.writeValueInCollection(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlAscii.readValue(bytes.iterator, VersionTwo)

    deserialisedValue should equal(None)
  }


  test("Reading value in collection") {
    val bytes = ByteString(Array[Byte](0, 2, 45, 43))

    val deserialisedValue = CqlAscii.readValueInCollection(bytes.iterator)

    deserialisedValue should equal("-+")
  }
}
