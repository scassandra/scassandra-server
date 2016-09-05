/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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

import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages.ProtocolVersion

case object CqlInt extends ColumnType[Integer](0x0009, "int") {

  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[Integer] = {
    Some(byteIterator.getInt)
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion): Array[Byte] = {
    val bs = ByteString.newBuilder
    val valueAsInt: Int = value match {
      case bd: BigDecimal => {
        if (bd.isValidInt) {
          bd.toInt
        } else {
          throw new IllegalArgumentException
        }
      }
      case asInt: Int => asInt
      case asString: String => asString.toInt
      case unknownType @ _ => throw new IllegalArgumentException(s"Can't serialise ${value} with type ${value.getClass} as Int")
    }
    bs.putInt(valueAsInt)
    bs.result().toArray
  }
}
