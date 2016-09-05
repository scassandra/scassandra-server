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

//todo change this to a types class
case class CqlSet[T](setType : ColumnType[T]) extends ColumnType[Set[_]](0x0022, s"set<${setType.stringRep}>") {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[Set[T]] = {
    val size = protocolVersion.collectionLength.getLength(byteIterator)

    // Deserialize each element into a list.
    Some(List.fill(size) {
      setType.readValueWithLength(byteIterator, inCollection=true).get
    }.toSet)
  }

  def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion) : Array[Byte] = {
    val seq: TraversableOnce[T] = value match {
      case _: TraversableOnce[T] =>
        value.asInstanceOf[TraversableOnce[T]]
      case _ =>
        throw new IllegalArgumentException(s"Can't serialise ${value} as Set of ${setType}")
    }

    val serializer: Any => Array[Byte] = setType.writeValueWithLength(_, inCollection=true)
    ColumnType.serializeCqlCollection(seq, serializer)
  }
}
