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
import akka.util.ByteString
import org.scassandra.server.cqlmessages.ProtocolProvider

class CqlUUIDTest extends FunSuite with Matchers with ProtocolProvider {

  test("Serialisation of CqlUUID") {
    CqlUUID.writeValue("59ad61d0-c540-11e2-881e-b9e6057626c4") should equal(Array(89, -83, 97, -48, -59, 64, 17, -30, -120, 30, -71, -26, 5, 118, 38, -60))

    intercept[IllegalArgumentException] {
      CqlUUID.writeValue("123")
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlUUID.writeValue(Map())
    }
  }
}
