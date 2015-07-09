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

import akka.util.ByteIterator
import org.scassandra.server.cqlmessages.ProtocolVersion

//todo change this to a types class
case class CqlList[T](listType : ColumnType[T]) extends ColumnType[Iterable[_]](0x0020, s"list<${listType.stringRep}>") {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[Iterable[T]] = {
    val size = protocolVersion.collectionLength.getLength(byteIterator)

    // Deserialize each element into a list.
    Some(List.fill(size) {
      listType.readValueWithLength(byteIterator, inCollection=true).get
    })
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion) : Array[Byte] = {
    val seq: TraversableOnce[T] = value match {
      case _: TraversableOnce[T] =>
        value.asInstanceOf[TraversableOnce[T]]
      case _ =>
        throw new IllegalArgumentException(s"Can't serialise ${value} as List of ${listType}")
    }

    val serializer: Any => Array[Byte] = listType.writeValueWithLength(_, inCollection=true)
    ColumnType.serializeCqlCollection(seq, serializer)
  }
}
