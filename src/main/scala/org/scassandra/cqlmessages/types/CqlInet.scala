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

import java.net.InetAddress
import akka.util.ByteIterator
import org.apache.cassandra.serializers.{InetAddressSerializer, FloatSerializer, TypeSerializer}
import org.scassandra.cqlmessages.CqlProtocolHelper
import com.google.common.net.InetAddresses

case object CqlInet extends ColumnType[InetAddress](0x0010, "inet") {
  override def readValue(byteIterator: ByteIterator): Option[InetAddress] = {
    CqlProtocolHelper.readInetValue(byteIterator)
  }

  def writeValue(value: Any) = {
    CqlProtocolHelper.serializeInetValue(InetAddresses.forString(value.toString))
  }

  override def convertToCorrectCollectionType(list: List[_]) : List[InetAddress] = {
    list.map {
      case bd: String => InetAddress.getByName(bd)
      case inet: InetAddress => inet
      case _ => throw new IllegalArgumentException("Expected string representing an inet address")
    }
  }

  override def serializer: TypeSerializer[InetAddress] = InetAddressSerializer.instance
}
