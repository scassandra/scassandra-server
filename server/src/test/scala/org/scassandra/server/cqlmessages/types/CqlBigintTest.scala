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
import org.scassandra.server.cqlmessages.ProtocolProvider

class CqlBigintTest extends FunSuite with Matchers with ProtocolProvider {

  test("Serialisation of CqlBigInt") {
    CqlBigint.writeValue(BigDecimal("123000000000")) should equal(Array(0, 0, 0, 28, -93, 95, 14, 0))
    CqlBigint.writeValue("123") should equal(Array[Byte](0, 0, 0, 0, 0, 0, 0, 123))

    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlBigint.writeValue(Map())
    }
  }
}
