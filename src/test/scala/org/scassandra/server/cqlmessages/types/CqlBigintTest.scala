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

class CqlBigintTest extends FunSuite with Matchers {

  test("Serialisation of CqlBigInt") {
    CqlBigint.writeValue(BigDecimal("123000000000")) should equal(Array(0, 0, 0, 8, 0, 0, 0, 28, -93, 95, 14, 0))
    CqlBigint.writeValue("123") should equal(Array[Byte](0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue("hello") should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(true) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(false) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(List()) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(Map()) should equal(Array[Byte](0, 0, 0, 4, 0, 0, 0, 123))
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlBigint.readValue(bytes.iterator, VersionTwo)

    deserialisedValue should equal(None)
  }
}
