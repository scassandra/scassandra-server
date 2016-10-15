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
package org.scassandra.codec.datatype

import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import org.scassandra.codec.ProtocolVersion
import scodec.Codec

class DataTypeSpec extends FlatSpec with Checkers with Matchers {

  implicit val protocolVersion = ProtocolVersion.latest

  def encodeAndDecode[T](codec: Codec[Any], data: T): Unit = {
    encodeAndDecode(codec, data, data)
  }

  def encodeAndDecode[T](codec: Codec[Any], data: T, expected: Any): Unit = {
    val encoded = codec.encode(data).require
    val decoded = codec.decodeValue(encoded).require

    val expectedResult = expected
    decoded shouldEqual expectedResult
  }

}
