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

import akka.util.ByteIterator
import org.scassandra.server.cqlmessages.ProtocolVersion

abstract class CqlTextType(override val code : Short, override val stringRep: String, val charSet: String = "UTF-8") extends ColumnType[String](code: Short, stringRep: String) {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[String] = {
    Some(byteIterator.toByteString.decodeString(charSet))
  }

  override def writeValue(value : Any)(implicit protocolVersion: ProtocolVersion) = {
    if (value.isInstanceOf[Iterable[_]] || value.isInstanceOf[Map[_,_]]) throw new IllegalArgumentException(s"Can't serialise ${value} as String")

    value.toString.getBytes(charSet)
  }
}
