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

class CqlInetTest extends FunSuite with Matchers with ProtocolProvider {
  test("Serialisation of CqlFloat") {
    CqlInet.writeValue("127.0.0.1") should equal(Array[Byte](127, 0, 0, 1))

    intercept[IllegalArgumentException] {
      CqlInet.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlInet.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlInet.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlInet.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlInet.writeValue(Map())
    }
  }
}
