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

import java.nio.ByteBuffer

import akka.util.ByteIterator
import org.apache.cassandra.serializers.{UTF8Serializer, AsciiSerializer, TypeSerializer}
import org.apache.cassandra.utils.ByteBufferUtil
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlVarchar extends ColumnType[String](0x000D, "varchar") {
   override def readValue(byteIterator: ByteIterator): Option[String] = {
     CqlProtocolHelper.readLongString(byteIterator)
   }

   override def writeValue(value : Any) = {
     if (value.isInstanceOf[Iterable[_]] || value.isInstanceOf[Map[_,_]]) throw new IllegalArgumentException(s"Can't serialise ${value} as String")

     val serialized = ByteBufferUtil.getArray(serializer.serialize(value.toString))
     CqlProtocolHelper.serializeInt(serialized.length) ++ serialized
   }

   override def writeValueInCollection(value : Any) = {
     CqlProtocolHelper.serializeString(value.toString)
   }

   override def readValueInCollection(byteIterator: ByteIterator) : String = {
     //todo handle null
     CqlProtocolHelper.readString(byteIterator)
   }

   override def serializer: TypeSerializer[String] = UTF8Serializer.instance
 }
