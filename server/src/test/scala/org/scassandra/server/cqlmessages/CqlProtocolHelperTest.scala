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
package org.scassandra.server.cqlmessages

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString
import java.util.UUID
import java.net.InetAddress

import org.scassandra.server.cqlmessages.types._

class CqlProtocolHelperTest extends FunSuite with Matchers with ProtocolProvider {

  test("Serialize short should produce two bytes") {
    CqlProtocolHelper.serializeShort(1) should equal(List(0x00, 0x01))
  }

  test("Serialize int should produce four bytes") {
    CqlProtocolHelper.serializeInt(1) should equal(List(0x00, 0x00, 0x00, 0x01))
  }

  test("Serialize string should produce produce length as two bytes then characters as ASCII") {
    CqlProtocolHelper.serializeString("CQL_VERSION") should
      equal(List(0, 11, 0x43, 0x51, 0x4c, 0x5f, 0x56, 0x45, 0x52, 0x53, 0x49, 0x4f, 0x4e))
  }

  test("Serializing [string list] value - (1,22,333,4444,55555)") {
    val list = List("1","22","333","4444","55555")
    val listAsBytes = CqlProtocolHelper.serializeStringList(list)
    listAsBytes should equal(Array[Byte](0x00, 0x5,
      0x00, 0x01, 0x31,
      0x00, 0x02, 0x32, 0x32,
      0x00, 0x03, 0x33, 0x33, 0x33,
      0x00, 0x04, 0x34, 0x34, 0x34, 0x34,
      0x00, 0x05, 0x35, 0x35, 0x35, 0x35, 0x35))
  }

  test("Serializing [string map] value - (1 -> 2, 3 -> 4, 5 -> 6)") {
    val map = Map("1" -> "2", "3" -> "4", "5" -> "6")
    val mapAsBytes = CqlProtocolHelper.serializeStringMap(map)

    val (lengthBytes, dataBytes) = mapAsBytes.splitAt(2)

    lengthBytes should equal(Array[Byte](0x00, 0x03))

    dataBytes.grouped(6).toList should contain only
      (Array[Byte](0x00, 0x01, 0x31, 0x00, 0x01, 0x32),
      Array[Byte](0x00, 0x01, 0x33, 0x00, 0x01, 0x34),
      Array[Byte](0x00, 0x01, 0x35, 0x00, 0x01, 0x36))
  }

  test("Serializing [string multimap] value - (1 -> [2], 3 -> [4, 5], 6 -> [7, 8, 9])") {
    val map = Map("1" -> Set("2"), "3" -> Set("4", "5"), "6" -> Set("7", "8", "9"))
    val mapAsBytes = CqlProtocolHelper.serializeStringMultiMap(map)

    val (lengthBytes, dataBytes) = mapAsBytes.splitAt(2)

    lengthBytes should equal(Array[Byte](0x00, 0x03))

    def matchesExpected(remaining: Array[Byte]): Unit = remaining match {
      case x if x.size == 0 => ()
      case _ =>
        val (lengthBytes, remainingBytes) = remaining.splitAt(2)
        lengthBytes should equal(Array[Byte](0x00, 0x01))
        val (keyBytes, remainingBytes2) = remainingBytes.splitAt(1)
        keyBytes(0) match {
          case 0x31      =>
            val (bytes, remainingBytes3) = remainingBytes2.splitAt(5)
            bytes should equal (Array[Byte](0x00, 0x01, 0x00, 0x01, 0x32))
            matchesExpected(remainingBytes3)
          case 0x33      =>
            val (bytes, remainingBytes3) = remainingBytes2.splitAt(8)
            bytes should equal (Array[Byte](0x00, 0x02, 0x00, 0x01, 0x34, 0x00, 0x01, 0x35))
            matchesExpected(remainingBytes3)
          case 0x36      =>
            val (bytes, remainingBytes3) = remainingBytes2.splitAt(11)
            bytes should equal (Array[Byte](0x00, 0x03, 0x00, 0x01, 0x37, 0x00, 0x01, 0x38, 0x00, 0x01, 0x39))
            matchesExpected(remainingBytes3)
          case value @ _ => fail(s"Unexpected value $value")
        }
    }

    matchesExpected(dataBytes)
  }

}
