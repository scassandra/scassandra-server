/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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

class CqlCounterTest extends FunSuite with Matchers with ProtocolProvider {
  test("Serialisation of CqlCounter") {
    CqlCounter.writeValue(BigDecimal("123000000000")) should equal(Array(0, 0, 0, 28, -93, 95, 14, 0))
    CqlCounter.writeValue("123") should equal(Array[Byte](0, 0, 0, 0, 0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlCounter.writeValue(Map())
    }
  }

}
