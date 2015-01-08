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

import java.lang

import akka.util.ByteIterator
import org.apache.cassandra.serializers.{DoubleSerializer, TypeSerializer}
import org.scassandra.server.cqlmessages.{ProtocolVersion, CqlProtocolHelper}

case object CqlDouble extends ColumnType[java.lang.Double](0x0007, "double") {
   override def readValue(byteIterator: ByteIterator, protocolVersion: ProtocolVersion): Option[java.lang.Double] = {
     CqlProtocolHelper.readDoubleValue(byteIterator).map(new lang.Double(_))
   }

   def writeValue( value: Any) = {
     CqlProtocolHelper.serializeDoubleValue(value.toString.toDouble)
   }

  override def convertToCorrectCollectionTypeForList(list: Iterable[_]) : List[java.lang.Double] = {
    list.map {
      case bd: BigDecimal => new java.lang.Double(bd.toDouble)
      case _ => throw new IllegalArgumentException("Expected list of BigDecimals")
    }.toList
  }

  override def serializer: TypeSerializer[java.lang.Double] = DoubleSerializer.instance
 }
