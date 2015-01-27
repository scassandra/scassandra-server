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

import akka.util.{ByteIterator}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.cassandra.serializers.TypeSerializer
import org.scassandra.cql._
import org.scassandra.server.cqlmessages.{VersionTwo, ProtocolVersion}

abstract class ColumnType[T](val code : Short, val stringRep: String) extends Logging {
  def readValue(byteIterator : ByteIterator, protocolVersion: ProtocolVersion) : Option[T]
  def writeValue(value : Any) : Array[Byte]
  def writeValueInCollection(value: Any) : Array[Byte] = ???
  def readValueInCollection(byteIterator: ByteIterator) : T = ???
  def serializer: TypeSerializer[T] = ???
  def convertToCorrectCollectionTypeForList(list: Iterable[_]) : List[T] = ???
  def convertToCorrectCollectionTypeForSet(set: Iterable[_]) : Set[T] = convertToCorrectCollectionTypeForList(set).toSet
}

object ColumnType extends Logging {
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

  def fromString(string: String) : Option[ColumnType[_]] = {

    val cqlTypeFactory = new CqlTypeFactory
    val cqlType = cqlTypeFactory.buildType(string)

    logger.info(s"Java type $cqlType")

    def convertJavaToScalaType(javaType: CqlType): ColumnType[_] = {
      javaType match {
        case primitive: PrimitiveType => ColumnTypeMapping(primitive.serialise())
        case map: MapType => new CqlMap(convertJavaToScalaType(map.getKeyType), convertJavaToScalaType(map.getValueType))
        case set: SetType => new CqlSet(convertJavaToScalaType(set.getType))
        case list: ListType => new CqlList(convertJavaToScalaType(list.getType))
      }
    }

    val parsedType = Some(convertJavaToScalaType(cqlType))
    logger.info(s"Type is $parsedType")
    parsedType
  }

}


