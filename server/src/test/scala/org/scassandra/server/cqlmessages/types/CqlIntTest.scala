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

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.server.cqlmessages.ProtocolProvider

class CqlIntTest extends FunSuite with Matchers with ProtocolProvider{

  test("Serialisation of CqlInt") {
    CqlInt.writeValue(BigDecimal("123")) should equal(Array[Byte](0, 0, 0, 123))
    CqlInt.writeValue("123") should equal(Array[Byte](0, 0, 0, 123))
    CqlInt.writeValue(123) should equal(Array[Byte](0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlInt.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(BigDecimal("12345678910"))
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlInt.writeValue(Map())
    }
  }
}
