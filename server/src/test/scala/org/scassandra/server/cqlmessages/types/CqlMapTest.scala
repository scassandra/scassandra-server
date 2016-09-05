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

import akka.util.ByteString
import org.scalatest.{FunSuite, Matchers}
import org.scassandra.server.cqlmessages.ProtocolProvider

class CqlMapTest extends FunSuite with Matchers with ProtocolProvider {
/*
Map: a [short] n indicating the size of the map, followed by n entries.
          Each entry is composed of two [short bytes] representing the key and
          the value of the entry map.
 */

  test("Reading a CqlMap[Varchar, Varchar] - empty") {
    val underTest = CqlMap(CqlVarchar, CqlVarchar)
    val serialisedMap = Array[Byte](
      0, 0)

    val result = underTest.readValue(ByteString(serialisedMap).iterator)

    result should equal(Some(Map()))
  }

  test("Reading a CqlMap[Varchar, Varchar] - two entries") {
    val underTest = CqlMap(CqlVarchar, CqlVarchar)
    val serialisedMap = Array[Byte](
      0, 2,         // number of elements in the map
      0, 3,   111, 110, 101,  // one
      0, 3,   116, 119, 111,  // two
      0, 5,   116, 104, 114, 101, 101, // three
      0, 4,   102, 111, 117, 114 // four

    )

    val result = underTest.readValue(ByteString(serialisedMap).iterator)

    result should equal(Some(Map("one" -> "two", "three" -> "four")))
  }

  test("Serialisation of CqlMap - Varchar") {
    val underTest = CqlMap(CqlVarchar, CqlVarchar)

    underTest.writeValue(Map("one" -> "two", "three" -> "four")) should equal(Array[Byte](
      0, 2,         // number of elements in the map
      0, 3,   111, 110, 101,  // one
      0, 3,   116, 119, 111,  // two
      0, 5,   116, 104, 114, 101, 101, // three
      0, 4,   102, 111, 117, 114 // four

    ))

    intercept[IllegalArgumentException] {
      underTest.writeValue(List())
    }

    intercept[IllegalArgumentException] {
      underTest.writeValue("0x01")
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue("1235")
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(BigDecimal("123.67"))
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue("hello")
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(true)
    }
    intercept[IllegalArgumentException] {
      underTest.writeValue(false)
    }
  }

  test("Serialization of CqlMap<Varchar, CqlSet<Varchar>>") {
    // Nested serialization test to ensure collections can be nested.
    val underTest = CqlMap(CqlVarchar, CqlSet(CqlVarchar))
    underTest.writeValue(Map()) should equal(Array[Byte](0,0))
    underTest.writeValue(Map("first" -> Set("one", "two", "three"), "second" -> Set("four", "five", "six"))) should equal(Array[Byte](
      0, 2, // number of pairs in map
      0, 5, 102, 105, 114, 115, 116, // first (key)
      0, 19, // byte length of set
      0, 3, // elements in set
      0, 3, 111, 110, 101,  // one
      0, 3, 116, 119, 111,  // two
      0, 5, 116, 104, 114, 101, 101, // three
      0, 6, 115, 101, 99, 111, 110, 100,
      0, 19, // byte length of second set
      0, 3, // elements in set
      0, 4, 102, 111, 117, 114, // four
      0, 4, 102, 105, 118, 101, // five
      0, 3, 115, 105, 120 // six
    ))
  }

}
