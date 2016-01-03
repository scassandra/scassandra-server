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

import java.net.InetAddress
import akka.util.{ByteString, ByteIterator}
import org.scassandra.server.cqlmessages.ProtocolVersion
import com.google.common.net.InetAddresses

case object CqlInet extends ColumnType[InetAddress](0x0010, "inet") {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[InetAddress] = {
    val bytes = byteIterator.toArray
    Some(InetAddress.getByAddress(bytes))
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion) = {
    val bs = ByteString.newBuilder
    val bytes = InetAddresses.forString(value.toString).getAddress
    bs.putBytes(bytes)
    bs.result().toArray
  }

  override def convertJsonToInternal(value: Any): Option[InetAddress] = {
    Option(InetAddresses.forString(value.toString))
  }
}
