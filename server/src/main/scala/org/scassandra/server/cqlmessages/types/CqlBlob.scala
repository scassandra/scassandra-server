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

import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages.ProtocolVersion
import org.scassandra.server.cqlmessages.CqlProtocolHelper.hex2Bytes

case object CqlBlob extends ColumnType[Array[Byte]](0x0003, "blob") {

   override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[Array[Byte]] = {
     Some(byteIterator.toArray)
   }

   override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion): Array[Byte] = {
     val bs = ByteString.newBuilder
     val array = hex2Bytes(value.toString)
     bs.putBytes(array)
     bs.result().toArray
   }
}
