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
package org.scassandra.server.priming

import org.scalatest.{Matchers, FunSuite}
import java.util.UUID
import org.scassandra.server.priming.json.PrimingJsonImplicits.AnyJsonFormat
import spray.json.{JsNull, JsNumber, JsString}
import java.net.InetAddress

class AnyJsonFormatTest extends FunSuite with Matchers {
  test("Write None") {
    AnyJsonFormat.write(None) should equal(JsNull)
  }

  test("Write Some") {
    val uuidToSerialise = UUID.randomUUID()
    AnyJsonFormat.write(Some(uuidToSerialise)) should equal(JsString(uuidToSerialise.toString))
  }

  test("Write UUID") {
    val uuidToSerialise = UUID.randomUUID()
    AnyJsonFormat.write(uuidToSerialise) should equal(JsString(uuidToSerialise.toString))
  }

  test("Write Array[Byte]") {
    val arrayToSerialise = Array[Byte](0, 18, 52, 84, 53, 52, 83, 69, 67, 84, 53)
    AnyJsonFormat.write(arrayToSerialise) should equal(JsString("0x0012345435345345435435"))
    AnyJsonFormat.write(Array[Byte]()) should equal(JsString("0x"))
  }

  test("Write BigInt") {
    val bigIntToSerialise = BigInt(1234)
    AnyJsonFormat.write(bigIntToSerialise) should equal(JsNumber(bigIntToSerialise))
  }

  test("Write java.math.BigDecimal") {
    val toSerialise = new java.math.BigDecimal("1234.56")
    AnyJsonFormat.write(toSerialise) should equal(JsString("1234.56"))
  }

  test("Write BigDecimal") {
    val toSerialise = BigDecimal("1234.56")
    AnyJsonFormat.write(toSerialise) should equal(JsString("1234.56"))
  }

  test("Write InetAddress") {
    val toSerialise = InetAddress.getByName("127.0.0.1")
    AnyJsonFormat.write(toSerialise) should equal(JsString("127.0.0.1"))
  }

  test("Write Float") {
    val toSerialise : Float = 50.0f
    AnyJsonFormat.write(toSerialise) should equal(JsString("50.0"))
  }
}
