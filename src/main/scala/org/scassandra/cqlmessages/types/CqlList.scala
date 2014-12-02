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

import java.nio.ByteBuffer
import java.util

import akka.util.ByteIterator
import org.apache.cassandra.serializers.{ListSerializer}
import org.apache.cassandra.utils.ByteBufferUtil
import org.scassandra.cqlmessages.CqlProtocolHelper
import org.scassandra.cqlmessages.CqlProtocolHelper._
import scala.collection.JavaConversions._

import scala.collection.mutable

//todo change this to a types class
case class CqlList[T](listType : ColumnType[T]) extends ColumnType[Iterable[_]](0x0020, s"list<${listType.stringRep}>") {
   override def readValue(byteIterator: ByteIterator): Option[Iterable[String]] = {
     CqlProtocolHelper.readVarcharSetValue(byteIterator)
   }

  def writeValue(value: Any) : Array[Byte] = {
    val setSerialiser: ListSerializer[T] = ListSerializer.getInstance(listType.serializer)
    val list: List[T] = value match {
      case _: Set[T] =>
        value.asInstanceOf[Set[T]].toList
      case _: List[T] =>
        value.asInstanceOf[List[T]]
      case _: Seq[T] =>
        value.asInstanceOf[Seq[T]].toList
      case _ =>
        throw new IllegalArgumentException(s"Can't serialise ${value} as List of ${listType}")
    }

    val collectionType: util.List[T] = listType.convertToCorrectCollectionTypeForList(list)

    val serialised: util.List[ByteBuffer] = setSerialiser.serializeValues(collectionType)

    val setContents = serialised.foldLeft(new Array[Byte](0))((acc, byteBuffer) => {
      val current: mutable.ArrayOps[Byte] = ByteBufferUtil.getArray(byteBuffer)
      acc ++ serializeShort(current.size.toShort) ++ current
    })

    serializeInt(setContents.length + 2) ++ serializeShort(list.size.toShort) ++ setContents
  }
 }
