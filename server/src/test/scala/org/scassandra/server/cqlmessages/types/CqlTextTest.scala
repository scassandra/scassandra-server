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

import org.scalatest.{Matchers, FunSuite}
import org.scassandra.server.cqlmessages.ProtocolProvider

class CqlTextTest extends FunSuite with Matchers with ProtocolProvider {

  test("Serialisation of CqlText") {
    CqlText.writeValue("hello") should equal(Array[Byte](104, 101, 108, 108, 111))
    CqlText.writeValue("") should equal(Array[Byte]())
    CqlText.writeValue(BigDecimal("123.67")) should equal(Array[Byte](49, 50, 51, 46, 54, 55))
    CqlText.writeValue(true) should equal(Array[Byte](116, 114, 117, 101))

    intercept[IllegalArgumentException] {
      CqlText.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlText.writeValue(Map())
    }
  }
}
