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

import akka.util.{ByteString, ByteIterator}
import org.scassandra.server.cqlmessages.ProtocolVersion

case object CqlVarint extends ColumnType[BigInt](0x000E, "varint") {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[BigInt] = {
    val bytes = byteIterator.toArray
    Some(BigInt(bytes))
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion): Array[Byte] = {
    val varint = BigInt(value.toString)
    val bs = ByteString.newBuilder
    val bytes = varint.toByteArray
    bs.putBytes(bytes)
    bs.result().toArray
  }
}
