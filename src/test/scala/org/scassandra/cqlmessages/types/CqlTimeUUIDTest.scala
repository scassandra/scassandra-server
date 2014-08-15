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

class CqlTimeUUIDTest extends FunSuite with Matchers {

  test("Serialisation of CqlTimeUUID") {
    CqlTimeUUID.writeValue("59ad61d0-c540-11e2-881e-b9e6057626c4") should equal(Array(0, 0, 0, 16, 89, -83, 97, -48, -59, 64, 17, -30, -120, 30, -71, -26, 5, 118, 38, -60))

    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue("123")
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlTimeUUID.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlTimeUUID.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }
}
