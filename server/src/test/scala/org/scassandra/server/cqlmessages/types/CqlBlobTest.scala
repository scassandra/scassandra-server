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

class CqlBlobTest extends FunSuite with Matchers with ProtocolProvider {

  test("Serialisation of CqlBlob") {
    CqlBlob.writeValue("0x01") should equal(Array(1))
    CqlBlob.writeValue("1235") should equal(Array(18, 53))

    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(false)
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(List())
    }
    intercept[IllegalArgumentException] {
      CqlBlob.writeValue(Map())
    }
  }
}
