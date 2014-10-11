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

import akka.util.{ByteString, ByteIterator}
import org.scassandra.cqlmessages.CqlProtocolHelper

// only supports strings for now.
//todo change this to a type class
case class CqlMap(keyType: ColumnType[_], valueType: ColumnType[_]) extends ColumnType[Map[Any, Any]](0x0021, s"map<${keyType.stringRep},${valueType.stringRep}>") {

  import CqlProtocolHelper._

  override def readValue(byteIterator: ByteIterator): Option[Map[Any, Any]] = {
    val setLength = byteIterator.getInt
    if (setLength == -1) {
      None
    } else {
      val numerOfElements = byteIterator.getShort
      val result = (0 until numerOfElements).map( index =>
        (keyType.readValueInCollection(byteIterator), valueType.readValueInCollection(byteIterator))
      ).toMap
      Some(result)
    }
  }

  def writeValue(value: Any): Array[Byte] = {
    if (value.isInstanceOf[Map[String, String]]) {
      val map = value.asInstanceOf[Map[String, String]]
      val builder = ByteString.newBuilder
      val size: Int = map.size
      builder.putShort(size)

      for (valueToSerialise: (String, String) <- map) {
        builder.putBytes(CqlProtocolHelper.serializeString(valueToSerialise._1))
        builder.putBytes(CqlProtocolHelper.serializeString(valueToSerialise._2))
      }


      val serialisedMap = builder.result().toArray
      CqlProtocolHelper.serializeInt(serialisedMap.size) ++ serialisedMap
    } else {
      throw new IllegalArgumentException(s"Can't serialise $value as map")
    }
  }
}

/*
[
-126, 0, 0, 8, // header
0, 0, 0, 104, // length
0, 0, 0, 2, // rows type
0, 0, 0, 1, // flags
0, 0, 0, 3, // col count
0, 6, // keyspace length
 112, 101, 111, 112, 108, 101, // people
0, 9, // table length
 109, 97, 112, 95, 116, 97, 98, 108, 101, //
0, 2, // col length name
105, 100, // id
0, 9, // type - int
0, 4, // col length
98, 108, 111, 98, // blob
0, 3, // type - blob
0, 8, // length -
109, 97, 112, 95, 116, 121, 112, 101, map_type
0, 33, type - map
0, 13, key type - varchar
0, 13, value type - varchar
0, 0, 0, 1, - row count
0, 0, 0, 4,
0, 0, 0, 1, int of length 4
0, 0, 0, 1,
18, // blob of length 1

0, 0, 0, 22, - total length
0, 1, // number of rows
0, 5, // length of key
99, 104, 114, 105, 115,
0, 11, // length of value
98, 97, 116, 101, 121, 115, 97, 102, 97, 115, 102]


 */
