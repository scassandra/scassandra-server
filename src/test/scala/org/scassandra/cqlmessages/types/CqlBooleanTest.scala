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
