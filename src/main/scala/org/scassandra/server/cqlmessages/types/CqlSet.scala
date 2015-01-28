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

import java.nio.ByteBuffer
import java.util

import akka.util.ByteIterator
import org.apache.cassandra.serializers.SetSerializer
import org.apache.cassandra.utils.ByteBufferUtil
import org.scassandra.server.cqlmessages.CqlProtocolHelper._
import org.scassandra.server.cqlmessages.ProtocolVersion

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

//todo change this to a types class
case class CqlSet[T](setType : ColumnType[T]) extends ColumnType[Set[_]](0x0022, s"set<${setType.stringRep}>") {
   override def readValue(byteIterator: ByteIterator, protocolVersion: ProtocolVersion): Option[Set[T]] = {
     val numberOfBytes = byteIterator.getInt
     if (numberOfBytes == -1) {
       None
     } else {
       val bytes = new Array[Byte](numberOfBytes)
       byteIterator.getBytes(bytes)
       Some(SetSerializer.getInstance(setType.serializer).deserializeForNativeProtocol(ByteBuffer.wrap(bytes), protocolVersion.version).asScala.toSet)
     }
   }

  def writeValue(value: Any) : Array[Byte] = {
    val setSerializer: SetSerializer[T] = SetSerializer.getInstance(setType.serializer)
    val set: Set[T] = value match {
      case s: Set[T] =>
        s
      case _: List[T] =>
        value.asInstanceOf[List[T]].toSet
      case _: Seq[T] =>
        value.asInstanceOf[Seq[T]].toSet
      case _ =>
        throw new IllegalArgumentException(s"Can't serialise $value as Set of $setType")
    }

    val collectionType: util.Set[T] = setType.convertToCorrectCollectionTypeForSet(set)

    val serialised: util.List[ByteBuffer] = setSerializer.serializeValues(collectionType)

    val setContents = serialised.foldLeft(new Array[Byte](0))((acc, byteBuffer) => {
      val current: mutable.ArrayOps[Byte] = ByteBufferUtil.getArray(byteBuffer)
      acc ++ serializeShort(current.size.toShort) ++ current
    })

    serializeInt(setContents.length + 2) ++ serializeShort(set.size.toShort) ++ setContents
  }

  override def convertToCorrectJavaTypeForSerializer(value: Any): Set[_] = throw new UnsupportedOperationException("Can't have sets in collections yet")
}
