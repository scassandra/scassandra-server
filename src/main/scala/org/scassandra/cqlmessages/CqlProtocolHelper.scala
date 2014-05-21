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

import akka.util.{ByteIterator, ByteString}
import java.util.UUID
import java.net.InetAddress
import scala.collection.immutable.IndexedSeq

object CqlProtocolHelper {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def serializeString(string: String) : Array[Byte] = {
    val bs = ByteString.newBuilder
    bs.putShort(string.length)
    bs.putBytes(string.getBytes())
    bs.result().toArray
  }

  def serializeLongString(string: String) : Array[Byte] = {
    val bs = ByteString.newBuilder
    bs.putInt(string.length)
    bs.putBytes(string.getBytes("UTF-8"))
    bs.result().toArray
  }

  def serializeByte(byte : Byte) : Array[Byte] = {
    Array(byte)
  }

  def serializeInt(int: Int) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(int)
    frameBuilder.result().toArray
  }

  def serializeShort(short : Short) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(short)
    frameBuilder.result().toArray
  }

  def serializeBooleanValue(bool : Boolean) : Array[Byte] = {
    Array[Byte](0,0,0,1,(if (bool) 1 else 0))
  }

  def serializeBigIntValue(value : Long) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(8)
    frameBuilder.putLong(value)
    frameBuilder.result().toArray
  }

  def serializeDecimalValue(decimal: java.math.BigDecimal) : Array[Byte] = {
    val scale = decimal.scale()
    val unscaledValue = decimal.unscaledValue().toByteArray
    val length = 4 + unscaledValue.length
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(length)
    frameBuilder.putInt(scale)
    frameBuilder.putBytes(unscaledValue)
    frameBuilder.result().toArray
  }

  def serializeDoubleValue(doubleValue : Double) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(8)
    frameBuilder.putDouble(doubleValue)
    frameBuilder.result().toArray
  }

  def serializeFloatValue(floatValue : Float) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(4)
    frameBuilder.putFloat(floatValue)
    frameBuilder.result().toArray
  }

  def serializeTimestampValue(timestampValue : Long) : Array[Byte] = {
    serializeBigIntValue(timestampValue)
  }

  def serializeUUIDValue(uuid : UUID) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(16)
    frameBuilder.putLong(uuid.getMostSignificantBits)
    frameBuilder.putLong(uuid.getLeastSignificantBits)
    frameBuilder.result().toArray
  }

  def serializeInetValue(inet : InetAddress) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    val bytes = inet.getAddress
    frameBuilder.putInt(bytes.length)
    frameBuilder.putBytes(bytes)
    frameBuilder.result().toArray
  }

  def serializeVarintValue(varint : BigInt) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    val bytes = varint.toByteArray
    frameBuilder.putInt(bytes.length)
    frameBuilder.putBytes(bytes)
    frameBuilder.result().toArray
  }

  def serializeVarcharSetValue(set : Iterable[String]) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(set.size)
    for (valueToSerialise <- set) {
      frameBuilder.putShort(valueToSerialise.size)
      frameBuilder.putBytes(valueToSerialise.getBytes())
    }
    val serialisedSet = frameBuilder.result().toArray
    serializeInt(serialisedSet.length) ++ serialisedSet
  }

  def readString(iterator: ByteIterator) : String = {
    val stringLength = iterator.getShort
    val stringBytes = new Array[Byte](stringLength)
    iterator.getBytes(stringBytes)
    new String(stringBytes)
  }

  def readLongString(iterator: ByteIterator) : String = {
    val stringLength = iterator.getInt
    val stringBytes = new Array[Byte](stringLength)
    iterator.getBytes(stringBytes)
    new String(stringBytes)
  }

  def readIntValue(iterator: ByteIterator) : Int = {
    val intLength = iterator.getInt
    iterator.getInt
  }

  def readBigIntValue(iterator: ByteIterator) : Long = {
    val intLength = iterator.getInt
    iterator.getLong
  }

  def readBooleanValue(iterator: ByteIterator) : Boolean = {
    val booleanLength = iterator.getInt
    val boolAsByte = iterator.getByte
    if (boolAsByte == 0) false else if (boolAsByte == 1) true else throw new IllegalArgumentException
  }

  def readBlobValue(iterator: ByteIterator) : Array[Byte] = {
    val length = iterator.getInt
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    bytes
  }

  def readDecimalValue(iterator: ByteIterator) : BigDecimal = {
    val length = iterator.getInt
    val scale = iterator.getInt
    val bytes = new Array[Byte](length - 4)
    iterator.getBytes(bytes)
    val unscaledValue = BigInt(bytes)
    BigDecimal(unscaledValue, scale)
  }

  def readDoubleValue(iterator: ByteIterator) : Double = {
    iterator.getInt
    iterator.getDouble
  }

  def readFloatValue(iterator: ByteIterator) : Float = {
    iterator.getInt
    iterator.getFloat
  }

  def readTimestampValue(iterator: ByteIterator) : Long = {
    iterator.getInt
    iterator.getLong
  }
  def readUUIDValue(iterator: ByteIterator) : UUID = {
    iterator.getInt
    val mostSignificantBytes = iterator.getLong
    val leastSignificantBytes = iterator.getLong
    new UUID(mostSignificantBytes, leastSignificantBytes)
  }
  def readInetValue(iterator: ByteIterator) : InetAddress = {
    val length = iterator.getInt
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    InetAddress.getByAddress(bytes)
  }
  def readVarintValue(iterator: ByteIterator) : BigInt = {
    val length = iterator.getInt
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    BigInt(bytes)
  }

  def readShortBytes(iterator: ByteIterator) : Array[Byte] = {
    val length = iterator.getShort
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    bytes
  }

  def readVarcharSetValue(iterator: ByteIterator)  = {
    val setLength = iterator.getInt
    val setSize = iterator.getShort
      (0 until setSize).map(it => {
        val bytes = readShortBytes(iterator)
        new String(bytes)
      }).toSet
  }
}

// example sets
/*
[-126, 0, 0, 8, // header
0, 0, 0, 92, // length
0, 0, 0, 2, // rows
0, 0, 0, 1, // flagz
0, 0, 0, 3, // col count
0, 6, // keyspace length
112, 101, 111, 112, 108, 101,
0, 9,
115, 101, 116, 95, 116, 97, 98, 108, 101,
0, 2, // length of id
105, 100, // id
0, 9, // id tyoe - int
0, 4, // lengh of blob
98, 108, 111, 98, // blob
0, 3, // blob
0, 7, length of   text set
116, 101, 120, 116, 115, 101, 116, // text set
0, 34, // set
0, 13, // type of set - varchar
 0, 0, 0, 1,  // number of rows
 0, 0, 0, 4, // id length
 0, 0, 0, 1,  // id value - 1
 0, 0, 0, 2, // blob length
 0, 4, //blob value
 0, 0, 0, 12, // set length
 0, 2, // length of set
  0, 3, 111, 110, 101, // one
  0, 3, 116, 119, 111] // two

 */


/*
[-126, 0, 0, 8, // result msg
0, 0, 1, 77, // length
0, 0, 0, 2, // result type = rows
0, 0, 0, 1, // flags = 1
0, 0, 0, 10, // col count - 10
0, 6, // keyspace name length
112, 101, 111, 112, 108, 101, // keyspace name
0, 11, // table name length
116, 121, 112, 101, 115, 95, 116, 97, 98, 108, 101, // table name
0, 2, 105, 100, 0, 9, ID - type 9 - int
0, 3, 98, 108, 111, 0, 3, - BLO - type 3 - blob
0, 3, 100, 101, 99, 0, 6, - type 6 - decimal
0, 3, 100, 111, 117, 0, 7,- type 7 - double
0, 3, 102, 108, 111, 0, 8, - type 8 - float
0, 3, 105, 110, 101, 0, 16, - type 16 - inet
0, 3, 116, 101, 120, 0, 13, - type 13 - text aka varchar
0, 3, 116, 105, 109, 0, 11, - type 11 - timestamp
0, 3, 117, 117, 105, 0, 12, - type 12 - uuid
0, 4, 118, 105, 110, 116, 0, 14, - type 14 - varint
0, 0, 0, 2, // number of rows - 2

0, 0, 0, 4,   0, 0, 0, 1,  -- int
0, 0, 0, 2,   0, 4, -- blob
0, 0, 0, 5,   0, 0, 0, 0, 5, -- decimal
0, 0, 0, 8,   64, 24, 0, 0, 0, 0, 0, 0, -- double
0, 0, 0, 4,   64, -32, 0, 0, -- float
    0, 0, 0, 16,  32, 1, 13, -72, -123, -93, 0, 66, 16, 0, -118, 46, 3, 112, 115, 52, -- inet - ipv6
0, 0, 0, 10,  104, 101, 108, 108, 111, 32, 116, 101, 120, 116, -- text
0, 0, 0, 8,   0, 0, 1, 62, -97, -67, 28, 56, -- timestamp
0, 0, 0, 16,  85, 14, -124, 0, -30, -101, 65, -44, -89, 22, 68, 102, 85, 68, 0, 0, - uuid
0, 0, 0, 1,   1, - var int

0, 0, 0, 4,   0, 0, 0, 2, -- int
0, 0, 0, 2,   0, 3, -- blob
0, 0, 0, 7,   0, 0, 0, 4, 0, -40, -96, -- decimal
0, 0, 0, 8,   64, 26, 102, 102, 102, 102, 102, 102, -- double
0, 0, 0, 4,   64, -10, 102, 102, -- float
0, 0, 0, 4,   127, 0, 0, 1, --inet - ipv4
0, 0, 0, 10,  104, 101, 108, 108, 111, 32, 116, 101, 120, 116, -- text
0, 0, 0, 8,   0, 0, 1, 62, -99, 69, 101, 120, -- timestamp
0, 0, 0, 16,  85, 14, -124, 0, -30, -101, 65, -44, -89, 22, 68, 102, 85, 68, 0, 0, -- uuid
0, 0, 0, 9,   6, -79, 78, -97, -107, -38, 26, -1, 87] - var intx§

[0, 3]
 */

/*
example timeuuid message
ByteString(-126, 0, 1, 8,
0, 0, 0, 66,
0, 0, 0, 2,
0, 0, 0, 1,
0, 0, 0, 1,
0, 6, 112, 101, 111, 112, 108, 101,
0, 14, 116, 105, 109, 101, 117, 117, 105, 100, 95, 116, 97, 98, 108, 101,
0, 2, 105, 100, 0, 15,
0, 0, 0, 1,
0, 0, 0, 16, 44, 83, 3, -128, -71, -7, 17, -29, -123, 14, 51, -117, -78, -94, -25, 79)
 */

