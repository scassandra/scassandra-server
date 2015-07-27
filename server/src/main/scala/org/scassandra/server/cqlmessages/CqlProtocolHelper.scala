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
package org.scassandra.server.cqlmessages

import java.nio.ByteOrder

import akka.util.{ByteIterator, ByteString, ByteStringBuilder}

/**
 * Provides helper functions used for serializing and deserializing
 * values defined in the Section 3 ('Notations') of the native protocol
 * spec.
 */
object CqlProtocolHelper {
  implicit val byteOrder : ByteOrder = ByteOrder.BIG_ENDIAN
  val NullValue: Array[Byte] = Array[Byte](-1, -1, -1, -1)

  /**
   * Serializes given [[String]] into [string] bytes.
   */
  def serializeString(string: String) : Array[Byte] = {
    val bs = ByteString.newBuilder
    bs.putShort(string.length)
    bs.putBytes(string.getBytes)
    bs.result().toArray
  }

  /**
   * Serializes given [[String]] into [long string] bytes.
   */
  def serializeLongString(string: String) : Array[Byte] = {
    val bs = ByteString.newBuilder
    bs.putInt(string.length)
    bs.putBytes(string.getBytes("UTF-8"))
    bs.result().toArray
  }

  /**
   * Serializes given [[Byte]] into [byte] bytes.
   */
  def serializeByte(byte : Byte) : Array[Byte] = {
    Array(byte)
  }

  /**
   * Serializes given [[Int]] into [int] bytes.
   */
  def serializeInt(int: Int) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(int)
    frameBuilder.result().toArray
  }

  /**
   * Serializes given [[Short]] into [short] bytes.
   */
  def serializeShort(short : Short) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(short)
    frameBuilder.result().toArray
  }

  /**
   * Serializes the given collection into a byte array using the given serializer for encoding
   * individual elements and size function for writing size of collection.
   * @param sizeF Function used for writing number of collection elements.
   * @param input Collection to serialize.
   * @param entrySerializer Serializer function that accepts a value and encodes it into bytes.
   * @tparam U type of elements being encoded.
   * @return The encoded bytes.
   */
  def serializeCollection[U](sizeF: (ByteStringBuilder, Int) => ByteStringBuilder)(input: TraversableOnce[U], entrySerializer: U => Array[Byte]) : Array[Byte] = {
    val frameBuilder = ByteString.newBuilder
    sizeF(frameBuilder, input.size)
    for (value <- input) {
      frameBuilder.putBytes(entrySerializer(value))
    }
    frameBuilder.result().toArray
  }

  /**
   * Serializes a collection of [String] into [string x].
   * @param input Collection to serialize.
   * @param entrySerializer Serializer function that accepts a value and encodes it into bytes.
   * @return The encoded bytes.
   */
  def serializeStringCollection[String](input: TraversableOnce[String], entrySerializer: String => Array[Byte]) =
    serializeCollection(_.putShort(_))(input, entrySerializer)

  /**
   * Serializes a list into [string list].
   * @param input List to serialize.
   * @return The encoded bytes.
   */
  def serializeStringList(input: List[String]) =
    serializeStringCollection(input, serializeString)

  /**
   * Serializes a map into [string map].
   * @param input Map to serialize.
   * @return The encoded bytes.
   */
  def serializeStringMap(input: Map[String, String]) =
    serializeStringCollection(input, (e: (String, String)) => serializeString(e._1) ++ serializeString(e._2))

  /**
   * Serializes a multi map into [string multimap].
   * @param input Map to serialize.
   * @return The encoded bytes.
   */
  def serializeStringMultiMap(input: Map[String, Set[String]]) =
    serializeStringCollection(input, (e: (String, Set[String])) => serializeString(e._1) ++ serializeStringList(e._2.toList))

  /**
   * Reads [string] bytes into [[String]].
   */
  def readString(iterator: ByteIterator) : String = {
    //todo handle null
    val stringLength = iterator.getShort
    val stringBytes = new Array[Byte](stringLength)
    iterator.getBytes(stringBytes)
    new String(stringBytes)
  }

  /**
   * Reads [long string] bytes into [[String]].
   */
  def readLongString(iterator: ByteIterator) : Option[String] = {
    val stringLength = iterator.getInt
    if (stringLength == -1) return None
    val stringBytes = new Array[Byte](stringLength)
    iterator.getBytes(stringBytes)
    Some(new String(stringBytes))
  }

  /**
   * Reads [int] into [[Int]].
   */
  def readIntValue(iterator: ByteIterator) : Option[Int] = {
    val intLength = iterator.getInt
    if (intLength == -1) return None
    Some(iterator.getInt)
  }

  /**
   * Reads [short bytes] into bytes.
   */
  def readShortBytes(iterator: ByteIterator) : Array[Byte] = {
    val length = iterator.getShort
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    bytes
  }

  /**
   * Determines whether or not into bytes represent null. (-1,-1,-1,-1 sequence)
   */
  def readNullValue(iterator: ByteIterator) : Boolean = {
    val bytes = new Array[Byte](4)
    iterator.getBytes(bytes)
    bytes.deep == Array[Byte](-1,-1,-1,-1).deep
  }

  /**
   * Returns a [[ByteString]] of header ++ [int length of body] ++ [body].
   */
  def combineHeaderAndLength(header: Array[Byte], body: Array[Byte]) : ByteString = {
    ByteString(header ++ serializeInt(body.length) ++ body)
  }

  /**
   * Reads an int then that many bytes
   * @param iterator
   */
  def consumeLongBytes(iterator: ByteIterator): Array[Byte] = {
    val length = iterator.getInt
    length match {
      case -1 => Array()
      case _  =>
        val bytes = new Array[Byte](length)
        iterator.getBytes(bytes)
        bytes
    }
  }

}

// example sets
/*
[-126, 0, 0, 8, // header
0, 0, 0, 92, // length
0, 0, 0, 2, // rows
0, 0, 0, 1, // flags
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

