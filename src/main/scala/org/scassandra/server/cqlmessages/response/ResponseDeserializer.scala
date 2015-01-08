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
package org.scassandra.server.cqlmessages.response

import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages._
import com.typesafe.scalalogging.slf4j.Logging
import java.nio.ByteOrder

object ResponseDeserializer extends Logging {

  implicit val byteOrder : ByteOrder = ByteOrder.BIG_ENDIAN

  val HeaderLength = 8

  def deserialize(byteString: ByteString): Response = {

    if (byteString.length < HeaderLength) throw new IllegalArgumentException

    val iterator = byteString.iterator
    val protocolVersionByte = iterator.getByte
    val flags = iterator.getByte
    val stream = iterator.getByte
    val opCode = iterator.getByte
    val bodyLength = iterator.getInt
    implicit val protocolVersion = ProtocolVersion.protocol(protocolVersionByte)

    if (iterator.len < bodyLength) throw new IllegalArgumentException

    opCode  match {
      case OpCodes.Ready => new Ready(stream)
      case OpCodes.Result => Result.fromByteString(byteString)
      case opCode @ _ =>
        throw new IllegalArgumentException(s"Received unknown opcode ${opCode}")
    }
  }

  def readString(iterator: ByteIterator, length : Int) = {
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    new String(bytes)
  }
}
