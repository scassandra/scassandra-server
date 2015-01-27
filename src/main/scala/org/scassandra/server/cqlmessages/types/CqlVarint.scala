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

import java.math.BigInteger

import akka.util.ByteIterator
import org.apache.cassandra.serializers.{IntegerSerializer, TypeSerializer}
import org.scassandra.server.cqlmessages.{ProtocolVersion, CqlProtocolHelper}

case object CqlVarint extends ColumnType[BigInteger](0x000E, "varint") {
   override def readValue(byteIterator: ByteIterator, protocolVersion: ProtocolVersion): Option[BigInteger] = {
     CqlProtocolHelper.readVarintValue(byteIterator).map(_.bigInteger)
   }

   def writeValue( value: Any): Array[Byte] = {
     CqlProtocolHelper.serializeVarintValue(BigInt(value.toString))
   }

  override def convertToCorrectCollectionTypeForList(list: Iterable[_]) : List[BigInteger] = {
    list.map(convertToCorrectJavaTypeForSerializer).toList
  }

  override def serializer: TypeSerializer[BigInteger] = IntegerSerializer.instance

  override def convertToCorrectJavaTypeForSerializer(value: Any): BigInteger = value match {
    case bd: String => new BigInteger(bd)
    case bigInt: BigInteger => bigInt
    case bd: BigDecimal => bd.toBigInt().bigInteger
    case bigInt: BigInt => bigInt.bigInteger
    case _ => throw new IllegalArgumentException("Expected string representing an BigInteger")
  }
}
