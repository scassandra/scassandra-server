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

import akka.util.{ByteIterator}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.cassandra.serializers.TypeSerializer

abstract class ColumnType[T](val code : Short, val stringRep: String) extends Logging {
  def readValue(byteIterator : ByteIterator) : Option[T]
  def writeValue(value : Any) : Array[Byte]
  def writeValueInCollection(value: Any) : Array[Byte] = ???
  def readValueInCollection(byteIterator: ByteIterator) : T = ???
  def serializer: TypeSerializer[T] = ???
  def convertToCorrectCollectionTypeForList(list: Iterable[_]) : List[T] = ???
  def convertToCorrectCollectionTypeForSet(set: Iterable[_]) : Set[T] = convertToCorrectCollectionTypeForList(set).toSet
}

object ColumnType {
  //todo change to pattern match
  val ColumnTypeMapping = Map[String, ColumnType[_]](
    CqlInt.stringRep -> CqlInt,
    CqlBoolean.stringRep -> CqlBoolean,
    CqlAscii.stringRep -> CqlAscii,
    CqlBigint.stringRep -> CqlBigint,
    CqlCounter.stringRep -> CqlCounter,
    CqlBlob.stringRep -> CqlBlob,
    CqlDecimal.stringRep -> CqlDecimal,
    CqlDouble.stringRep -> CqlDouble,
    CqlFloat.stringRep -> CqlFloat,
    CqlText.stringRep -> CqlText,
    CqlTimestamp.stringRep -> CqlTimestamp,
    CqlUUID.stringRep -> CqlUUID,
    CqlInet.stringRep -> CqlInet,
    CqlVarint.stringRep -> CqlVarint,
    CqlTimeUUID.stringRep -> CqlTimeUUID,
    CqlVarchar.stringRep -> CqlVarchar,
    "set" -> CqlSet(CqlVarchar),
    "set<varchar>" -> CqlSet(CqlVarchar),
    "set<ascii>" -> CqlSet(CqlAscii),
    "set<text>" -> CqlSet(CqlText),
    "set<int>" -> CqlSet(CqlInt),
    "set<bigint>" -> CqlSet(CqlBigint),
    "set<boolean>" -> CqlSet(CqlBoolean),
    "set<counter>" -> CqlSet(CqlCounter),
    "set<decimal>" -> CqlSet(CqlDecimal),
    "set<double>" -> CqlSet(CqlDouble),
    "set<float>" -> CqlSet(CqlFloat),
    "set<inet>" -> CqlSet(CqlInet),
    "set<timestamp>" -> CqlSet(CqlTimestamp),
    "set<uuid>" -> CqlSet(CqlUUID),
    "set<timeuuid>" -> CqlSet(CqlTimeUUID),
    "set<varint>" -> CqlSet(CqlVarint),
    "set<blob>" -> CqlSet(CqlBlob),
    "list" -> CqlList(CqlVarchar),
    "list<varchar>" -> CqlList(CqlVarchar),
    "list<ascii>" -> CqlList(CqlAscii),
    "list<text>" -> CqlList(CqlText),
    "list<int>" -> CqlList(CqlInt),
    "list<bigint>" -> CqlList(CqlBigint),
    "list<boolean>" -> CqlList(CqlBoolean),
    "list<counter>" -> CqlList(CqlCounter),
    "list<decimal>" -> CqlList(CqlDecimal),
    "list<double>" -> CqlList(CqlDouble),
    "list<float>" -> CqlList(CqlFloat),
    "list<inet>" -> CqlList(CqlInet),
    "list<timestamp>" -> CqlList(CqlTimestamp),
    "list<uuid>" -> CqlList(CqlUUID),
    "list<timeuuid>" -> CqlList(CqlTimeUUID),
    "list<varint>" -> CqlList(CqlVarint),
    "list<blob>" -> CqlList(CqlBlob),
    "map<varchar,varchar>" -> CqlMap(CqlVarchar, CqlVarchar),
    "map<varchar,text>" -> CqlMap(CqlVarchar, CqlText),
    "map<varchar,ascii>" -> CqlMap(CqlVarchar, CqlAscii),
    "map<ascii,ascii>" -> CqlMap(CqlAscii, CqlAscii),
    "map<ascii,text>" -> CqlMap(CqlAscii, CqlText),
    "map<ascii,varchar>" -> CqlMap(CqlAscii, CqlVarchar),
    "map<text,text>" -> CqlMap(CqlText, CqlText),
    "map<text,varchar>" -> CqlMap(CqlText, CqlVarchar),
    "map<text,ascii>" -> CqlMap(CqlText, CqlAscii)
  )

  def fromString(string: String) : Option[ColumnType[_]] = {
    ColumnTypeMapping.get(string.toLowerCase())
  }

}


