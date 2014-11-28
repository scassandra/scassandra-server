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
import org.apache.cassandra.serializers.{Int32Serializer, TypeSerializer}
import org.scassandra.cqlmessages.CqlProtocolHelper
import scala.collection.JavaConversions._

case object CqlInt extends ColumnType[Integer](0x0009, "int") {

  import CqlProtocolHelper._

  override def readValue(byteIterator: ByteIterator): Option[Integer] = {
    CqlProtocolHelper.readIntValue(byteIterator).map(_.toInt)
  }

  override def convertToCorrectCollectionType(list: List[_]) : List[Integer] = {
    list.map {
      case bd: BigDecimal => new Integer(bd.toInt)
      case _ => throw new IllegalArgumentException("Expected list of BigDecimals")
    }
  }

  def writeValue(value: Any): Array[Byte] = {
    val bs = ByteString.newBuilder
    bs.putInt(4)
    val valueAsInt = value match {
      case bd: BigDecimal => {
        if (bd.isValidInt) {
          bd.toInt
        } else {
          throw new IllegalArgumentException
        }
      }
      case asString: String => asString.toInt
      case unknownType@_ => throw new IllegalArgumentException(s"Can't serialise ${value} with type ${value.getClass} as Int")
    }
    bs.putInt(valueAsInt)
    bs.result().toArray
  }

  override def serializer: TypeSerializer[Integer] = Int32Serializer.instance
}
