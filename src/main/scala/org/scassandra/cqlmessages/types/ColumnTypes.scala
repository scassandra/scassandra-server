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

abstract class ColumnType[T](val code : Short, val stringRep: String) extends Logging {
  def readValue(byteIterator : ByteIterator) : T
  def writeValue(value : Any) : Array[Byte]
  def writeValueInCollection(value: Any) : Array[Byte] = ???
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
    "list" -> CqlList(CqlVarchar),
    "list<varchar>" -> CqlList(CqlVarchar),
    "list<ascii>" -> CqlList(CqlAscii),
    "list<text>" -> CqlList(CqlText),
    "map<varchar,varchar>" -> CqlMap(CqlVarchar, CqlVarchar),
    "map<varchar,text>" -> CqlMap(CqlVarchar, CqlText),
    "map<varchar,ascii>" -> CqlMap(CqlVarchar, CqlAscii),
    "map<ascii,asiii>" -> CqlMap(CqlAscii, CqlAscii),
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


