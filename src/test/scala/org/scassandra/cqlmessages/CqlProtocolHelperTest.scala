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
package org.scassandra.cqlmessages

import org.scalatest.{FunSuite, Matchers}
import akka.util.ByteString
import java.util.UUID
import java.net.InetAddress

class CqlProtocolHelperTest extends FunSuite with Matchers {

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

  test("Reading decimal value - 5.5456") {
    val decimalAsBytes = Array[Byte](0, 0, 0, 7, 0, 0, 0, 4, 0, -40, -96)
    val decimal = CqlProtocolHelper.readDecimalValue(ByteString(decimalAsBytes).iterator)
    decimal should equal(Some(BigDecimal("5.5456")))
  }
  test("Reading decimal value - 5") {
    val decimalAsBytes = Array[Byte](0, 0, 0, 5, 0, 0, 0, 0, 5)
    val decimal = CqlProtocolHelper.readDecimalValue(ByteString(decimalAsBytes).iterator)
    decimal.get should equal(BigDecimal("5"))
  }

  test("Serializing decimal value - 5.5456") {
    val decimal = new java.math.BigDecimal("5.5456")
    val decimalAsBytes = CqlProtocolHelper.serializeDecimalValue(decimal)
    decimalAsBytes should equal(Array[Byte](0, 0, 0, 7, 0, 0, 0, 4, 0, -40, -96))
  }
  test("Serializing decimal value - 5") {
    val decimal = new java.math.BigDecimal("5")
    val decimalAsBytes = CqlProtocolHelper.serializeDecimalValue(decimal)
    decimalAsBytes should equal(Array[Byte](0, 0, 0, 5, 0, 0, 0, 0, 5))
  }

  test("Serializing double value - 6.6") {
    val double : Double = 6.6
    val doubleAsBytes = CqlProtocolHelper.serializeDoubleValue(double)
    doubleAsBytes should equal(Array[Byte](0, 0, 0, 8, 64, 26, 102, 102, 102, 102, 102, 102))
  }
  test("Reading double value - 6.6") {
    val doubleAsBytes = Array[Byte](0, 0, 0, 8, 64, 26, 102, 102, 102, 102, 102, 102)
    val double = CqlProtocolHelper.readDoubleValue(ByteString(doubleAsBytes).iterator)
    double.get should equal(6.6)
  }

  test("Serializing float value - 7.7") {
    val float : Float = 7.7f
    val doubleAsBytes = CqlProtocolHelper.serializeFloatValue(float)
    doubleAsBytes should equal(Array[Byte](0, 0, 0, 4,   64, -10, 102, 102))
  }

  test("Reading float value - 7.7") {
    val floatAsBytes = Array[Byte](0, 0, 0, 4,   64, -10, 102, 102)
    val float = CqlProtocolHelper.readFloatValue(ByteString(floatAsBytes).iterator)
    float.get should equal(7.7f)
  }

  test("Reading timestamp value - 1368438171000") {
    val timestampAsBytes = Array[Byte](0, 0, 0, 8, 0, 0, 1, 62, -99, 69, 101, 120)
    val timestamp = CqlProtocolHelper.readTimestampValue(ByteString(timestampAsBytes).iterator)
    timestamp.get should equal(1368438171000l)
  }
  test("Serializing timestamp value - 1368438171000") {
    val timestamp : Long = 1368438171000l
    val timestampAsBytes = CqlProtocolHelper.serializeTimestampValue(timestamp)
    timestampAsBytes should equal(Array[Byte](0, 0, 0, 8, 0, 0, 1, 62, -99, 69, 101, 120))
  }

  test("Reading uuid value - 550e8400-e29b-41d4-a716-446655440000") {
    val uuidAsBytes = Array[Byte](0, 0, 0, 16,  85, 14, -124, 0, -30, -101, 65, -44, -89, 22, 68, 102, 85, 68, 0, 0)
    val uuid = CqlProtocolHelper.readUUIDValue(ByteString(uuidAsBytes).iterator)
    uuid.get should equal(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"))
  }
  test("Serializing uuid value - 550e8400-e29b-41d4-a716-446655440000") {
    val uuid : UUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val uuidAsBytes = CqlProtocolHelper.serializeUUIDValue(uuid)
    uuidAsBytes should equal(Array[Byte](0, 0, 0, 16,  85, 14, -124, 0, -30, -101, 65, -44, -89, 22, 68, 102, 85, 68, 0, 0))
  }

  test("Reading inet value - 127.0.0.1") {
    val uuidAsBytes = Array[Byte](0, 0, 0, 4, 127, 0, 0, 1)
    val inet = CqlProtocolHelper.readInetValue(ByteString(uuidAsBytes).iterator)
    inet.get should equal(InetAddress.getByAddress(Array[Byte](127,0,0,1)))
  }

  test("Reading inet value - 2001:db8:85a3:42:1000:8a2e:370:7334") {
    val uuidAsBytes = Array[Byte](0, 0, 0, 16,  32, 1, 13, -72, -123, -93, 0, 66, 16, 0, -118, 46, 3, 112, 115, 52)
    val inet = CqlProtocolHelper.readInetValue(ByteString(uuidAsBytes).iterator)
    inet.get should equal(InetAddress.getByAddress(Array[Byte](32, 1, 13, -72, -123, -93, 0, 66, 16, 0, -118, 46, 3, 112, 115, 52)))
  }

  test("Serializing inet value - 2001:db8:85a3:42:1000:8a2e:370:7334") {
    val inet : InetAddress = InetAddress.getByAddress(Array[Byte](32, 1, 13, -72, -123, -93, 0, 66, 16, 0, -118, 46, 3, 112, 115, 52))
    val inetAsBytes = CqlProtocolHelper.serializeInetValue(inet)
    inetAsBytes should equal(Array[Byte](0, 0, 0, 16,  32, 1, 13, -72, -123, -93, 0, 66, 16, 0, -118, 46, 3, 112, 115, 52))
  }

  test("Serializing varint value - 123456789101112131415") {
    val varint : BigInt = BigInt("123456789101112131415")
    val varintAsBytes = CqlProtocolHelper.serializeVarintValue(varint)
    varintAsBytes should equal(Array[Byte](0, 0, 0, 9, 6, -79, 78, -97, -107, -38, 26, -1, 87))
  }
  test("Reading varint value - 123456789101112131415") {
    val varintAsBytes = Array[Byte](0, 0, 0, 9, 6, -79, 78, -97, -107, -38, 26, -1, 87)
    val varint = CqlProtocolHelper.readVarintValue(ByteString(varintAsBytes).iterator)
    varint.get should equal(BigInt("123456789101112131415"))
  }
  test("Reading a set of strings") {
    val serialisedSet = Array[Byte](0, 0, 0, 12, // set length
    0, 2, // length of set
    0, 3, 111, 110, 101, // one
    0, 3, 116, 119, 111) // two

    val set = CqlProtocolHelper.readVarcharSetValue(ByteString(serialisedSet).iterator)

    set.get should equal(Set("one", "two"))
  }
}
