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
package org.scassandra.server.cqlmessages.types

import akka.util.ByteIterator
import org.apache.cassandra.serializers.{UTF8Serializer, TypeSerializer}
import org.scassandra.server.cqlmessages.ProtocolVersion

case object CqlText extends ColumnType[String](0x000A, "text") {
   override def readValue(byteIterator: ByteIterator, protocolVersion: ProtocolVersion): Option[String] = {
     CqlVarchar.readValue(byteIterator, protocolVersion)
   }

   override def writeValue(value : Any): Array[Byte] = {
     CqlVarchar.writeValue(value)
   }

   override def writeValueInCollection(value : Any) = {
     CqlVarchar.writeValueInCollection(value)
   }

   override def readValueInCollection(byteIterator: ByteIterator): String = {
     CqlVarchar.readValueInCollection(byteIterator)
   }

  override def convertToCorrectCollectionTypeForList(list: Iterable[_]) : List[String] = {
    list.map(_.toString).toList
  }

  override def serializer: TypeSerializer[String] = UTF8Serializer.instance

  override def convertToCorrectJavaTypeForSerializer(value: Any): String = value.toString
}
