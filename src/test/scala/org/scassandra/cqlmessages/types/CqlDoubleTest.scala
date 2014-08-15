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

class CqlDoubleTest extends FunSuite with Matchers {

  test("Serialisation of CqlDouble") {
    CqlDouble.writeValue(BigDecimal("123")) should equal(Array(0, 0, 0, 8, 64, 94, -64, 0, 0, 0, 0, 0))
    CqlDouble.writeValue("123") should equal(Array(0, 0, 0, 8, 64, 94, -64, 0, 0, 0, 0, 0))
    CqlDouble.writeValue(BigDecimal("123.67")) should equal(Array(0, 0, 0, 8, 64, 94, -22, -31, 71, -82, 20, 123))

    intercept[IllegalArgumentException] {
      CqlDouble.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlDouble.writeValue(Map())
    }
  }

  test("Reading null") {
    val bytes = ByteString(Array[Byte](-1,-1,-1,-1))
    val deserialisedValue = CqlDouble.readValue(bytes.iterator)

    deserialisedValue should equal(None)
  }

}
