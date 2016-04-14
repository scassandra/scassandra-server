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

import akka.util.{ByteStringBuilder, ByteString, ByteIterator}
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.cql._
import org.scassandra.server.cqlmessages.{ProtocolVersion, CqlProtocolHelper}

import scala.util.{Failure, Success, Try}

abstract class ColumnType[T](val code : Short, val stringRep: String) extends LazyLogging {
  implicit val byteOrder = CqlProtocolHelper.byteOrder

  /**
   * Deserializes given bytes into it's respective value if it can and returns it (or None).
   * @param byteIterator An iterator containing only bytes for this value.
   *                     It is appropriate for the entire iterator to be consumed.
   * @param protocolVersion Protocol version to decode with.
   * @return The decoded value.
   */
  def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion) : Option[T]

  /**
   * Takes a sequence of bytes, decodes bytes to determine length and then consumes length bytes from
   * iterator and passes them to [[readValue]].
   * @param byteIterator An iterator to consume bytes from.
   * @param inCollection Whether or not the value is a member of a collection.  If member of a
   *                     collection will use [[ProtocolVersion.collectionLength.getLength]] to
   *                     determine number of bytes to parse for length.  Otherwise if the value
   *                     is not member of a collection it will parse [int] bytes.
   * @param protocolVersion Protocol version to decode with.
   * @return The decoded value.
   */
  final def readValueWithLength(byteIterator: ByteIterator, inCollection: Boolean = false)(implicit protocolVersion: ProtocolVersion): Option[T] = {
    val length = inCollection match {
      case true => protocolVersion.collectionLength.getLength(byteIterator)
      case false => byteIterator.getInt
    }

    length match {
      case -1 => None
      case _  =>
        val bytes = new Array[Byte](length)
        byteIterator.getBytes(bytes)
        val slice = ByteString.newBuilder.putBytes(bytes).result().iterator
        readValue(slice)(protocolVersion)
    }
  }

  /**
   * Serializes given value into an byte array.
   * @param value Value to serialize.
   * @param protocolVersion Protocol version to encode with.
   * @return The encoded bytes.
   */
  def writeValue(value : Any)(implicit protocolVersion: ProtocolVersion) : Array[Byte]

  /**
   * Serializes given value into a byte array preceded by byte length.
   * @param value Value to serialize.
   * @param inCollection Whether or not the value is a member of a collection.  If member of a
   *                     collection will use [[ProtocolVersion.collectionLength.putLength]] to
   *                     encode length bytes.  Otherwise if the value is not member of a
   *                     collection it will encode [int] bytes.
   * @param protocolVersion Protocol verson to encode with.
   * @return The encoded bytes.
   */
  final def writeValueWithLength(value: Any, inCollection: Boolean = false)(implicit protocolVersion: ProtocolVersion): Array[Byte] = {
    val sizeF: (ByteStringBuilder, Int) => ByteStringBuilder = inCollection match {
      case true => protocolVersion.collectionLength.putLength
      case false => _.putInt(_)
    }

    val bytes = writeValue(value)
    val bs = sizeF(ByteString.newBuilder, bytes.length)
    bs.putBytes(bytes)
    bs.result().toArray
  }

  /**
    * Converts the priming json representation into the internal representation T
    *
    * If the JSON type matches the internal representation then this method need not
    * be overridden.
    *
    * @return the decoded value or None for null
    */
  def convertJsonToInternal(value: Any): Option[T] = {
    Option(value.asInstanceOf[T])
  }
}

object ColumnType extends LazyLogging {
  //todo change to pattern match
  val ColumnTypeMapping = Map[String, ColumnType[_]](
    CqlInt.stringRep -> CqlInt,
    CqlBoolean.stringRep -> CqlBoolean,
    CqlAscii.stringRep -> CqlAscii,
    CqlBigint.stringRep -> CqlBigint,
    CqlCounter.stringRep -> CqlCounter,
    CqlBlob.stringRep -> CqlBlob,
    CqlDecimal.stringRep -> CqlDecimal,
    CqlDouble.stringRep -> CqlDouble,
    CqlFloat.stringRep -> CqlFloat,
    CqlText.stringRep -> CqlText,
    CqlTimestamp.stringRep -> CqlTimestamp,
    CqlUUID.stringRep -> CqlUUID,
    CqlInet.stringRep -> CqlInet,
    CqlVarint.stringRep -> CqlVarint,
    CqlTimeUUID.stringRep -> CqlTimeUUID,
    CqlVarchar.stringRep -> CqlVarchar
  )

  /**
   * Serializes the given collection into a byte array using the given serializer for encoding
   * individual elements.
   * @param input Collection to serialize.
   * @param entrySerializer Serializer function that accepts a value and encodes it into bytes
   *                        preceded by length bytes.
   * @param protocolVersion Protocol version to encode with.
   * @tparam U type of elements being encoded.
   * @return The encoded bytes.
   */
  def serializeCqlCollection[U](input: TraversableOnce[U], entrySerializer: Any => Array[Byte])(implicit protocolVersion: ProtocolVersion) = {
    val serializer: (TraversableOnce[U], U => Array[Byte]) => Array[Byte] =
      CqlProtocolHelper.serializeCollection(protocolVersion.collectionLength.putLength)
    serializer(input, entrySerializer)
  }

  def fromString(string: String): Try[ColumnType[_]] = {

    def convertJavaToScalaType(javaType: CqlType): ColumnType[_] = {
      javaType match {
        case primitive: PrimitiveType => ColumnTypeMapping(primitive.serialise())
        case map: MapType => new CqlMap(convertJavaToScalaType(map.getKeyType), convertJavaToScalaType(map.getValueType))
        case set: SetType => new CqlSet(convertJavaToScalaType(set.getType))
        case list: ListType => new CqlList(convertJavaToScalaType(list.getType))
      }
    }

    val cqlTypeFactory = new CqlTypeFactory

    try {
      val cqlType = cqlTypeFactory.buildType(string)

      logger.trace(s"Java type $cqlType")
      val parsedType = Success(convertJavaToScalaType(cqlType))
      logger.info(s"Type is $parsedType")
      parsedType
    } catch {
      case e: Exception => Failure(e)
    }
  }
}