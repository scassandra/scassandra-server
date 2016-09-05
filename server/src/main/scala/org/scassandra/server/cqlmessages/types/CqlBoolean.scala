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

case object CqlBoolean extends ColumnType[java.lang.Boolean](0x0004, "boolean") {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[java.lang.Boolean] = {
    val boolAsByte = byteIterator.getByte
    if (boolAsByte == 0) Some(false)
    else if (boolAsByte == 1) Some(true)
    else throw new IllegalArgumentException
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion) = {
    Array[Byte](if (value.toString.toBoolean) 1 else 0)
  }
}
