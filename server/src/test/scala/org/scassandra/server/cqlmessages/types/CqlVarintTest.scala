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

class CqlVarintTest extends FunSuite with Matchers with ProtocolProvider {

  test("Serialisation of CqlVarint") {
    CqlVarint.writeValue(BigDecimal("123000000000")) should equal(Array(28, -93, 95, 14, 0))
    CqlVarint.writeValue("123") should equal(Array(123))

    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlVarint.writeValue(Map())
    }
  }
}
