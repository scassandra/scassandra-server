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
package org.scassandra.server.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}
import akka.util.{ByteString, ByteIterator}
import org.scassandra.server.cqlmessages.types.{CqlVarchar, CqlMap}

class ResultHelperTest extends FunSuite with Matchers { 
  test("Serialising map type") {
    //given
    val byteBuilder = ByteString.newBuilder
    //when
    ResultHelper.serialiseTypeInfomration("map_type", new CqlMap(CqlVarchar, CqlVarchar), byteBuilder)
    //then
    byteBuilder.result() should equal(ByteString(0, 8, 109, 97, 112, 95, 116, 121, 112, 101, 0, 33, 0, 13, 0, 13))
  }  
}
