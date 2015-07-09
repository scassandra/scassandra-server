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
import akka.util.ByteString
import org.scassandra.server.cqlmessages.ProtocolProvider

class CqlFloatTest extends FunSuite with Matchers with ProtocolProvider {
  test("Serialisation of CqlFloat") {
    CqlFloat.writeValue(BigDecimal("123")) should equal(Array(66, -10, 0, 0))
    CqlFloat.writeValue("123") should equal(Array(66, -10, 0, 0))
    CqlFloat.writeValue(BigDecimal("123.67")) should equal(Array(66, -9, 87, 10))

    intercept[IllegalArgumentException] {
      CqlFloat.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlFloat.writeValue(Map())
    }
  }

}
