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
package org.scassandra.server.cqlmessages.request

import org.scassandra.server.cqlmessages._
import akka.util.{ByteIterator, ByteString}
import org.scassandra.server.cqlmessages.types.ColumnType

object ExecuteRequest {

  import CqlProtocolHelper._

  def versionTwoWithoutTypes(stream: Byte, byteString: ByteString) : ExecuteRequestV2 = {
    val bodyIterator: ByteIterator = byteString.iterator
    val preparedStatementId = readPreparedStatementId(bodyIterator)
    val consistency = Consistency.fromCode(bodyIterator.getShort)
    val flags = bodyIterator.getByte
    val numberOfVariables = bodyIterator.getShort
    ExecuteRequestV2(ProtocolVersion.ClientProtocolVersionTwo, stream, preparedStatementId, consistency, numberOfVariables, List(), flags)
  }

  def versionTwoWithTypes(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]) : ExecuteRequestV2 = {
    val bodyIterator: ByteIterator = byteString.iterator
    val preparedStatementId = readPreparedStatementId(bodyIterator)
    val consistency = Consistency.fromCode(bodyIterator.getShort)
    val flags = bodyIterator.getByte
    val numberOfVariables = bodyIterator.getShort

    val variableValues = variableTypes.map (varType => {
      varType.readValueWithLength(bodyIterator)(VersionTwo)
    })
    ExecuteRequestV2(ProtocolVersion.ClientProtocolVersionTwo, stream, preparedStatementId, consistency, numberOfVariables, variableValues, flags)
  }

  def versionOneWithoutTypes(stream: Byte, byteString: ByteString) : ExecuteRequestV1 = {
    val bodyIterator: ByteIterator = byteString.iterator
    // length of the id - this is a short
    bodyIterator.drop(2)
    val preparedStatementId = bodyIterator.getInt
    val numberOfVariables = bodyIterator.getShort
    val consistencyByteString = bodyIterator.toByteString.takeRight(2)
    val byteShort: Short = consistencyByteString.iterator.getShort
    val consistency = Consistency.fromCode(byteShort)
    ExecuteRequestV1(ProtocolVersion.ClientProtocolVersionOne, stream, preparedStatementId, consistency, numberOfVariables, List())
  }

  def versionOneWithTypes(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]) : ExecuteRequestV1 = {
    val bodyIterator: ByteIterator = byteString.iterator
    // length of the id - this is a short
    bodyIterator.drop(2)
    val preparedStatementId = bodyIterator.getInt
    val numberOfVariables = bodyIterator.getShort


    val variableValues = variableTypes.map (varType => {
      varType.readValueWithLength(bodyIterator)(VersionOne)
    } )
    val consistency = Consistency.fromCode(bodyIterator.getShort)
    ExecuteRequestV1(ProtocolVersion.ClientProtocolVersionOne, stream, preparedStatementId, consistency, numberOfVariables, variableValues)
  }
}

trait ExecuteRequest {
  val protocolVersion: Byte
  val stream: Byte
  val id: Int
  val consistency: Consistency
  val numberOfVariables : Int
  val variables : List[Any]
}

case class ExecuteRequestV2(protocolVersion: Byte, stream: Byte, id: Int, consistency : Consistency = ONE, numberOfVariables: Int = 0, variables : List[Any] = List(), flags : Byte = 0x00, variableTypes: List[ColumnType[_]] = List()) extends Request(new Header(protocolVersion, OpCodes.Execute, stream)) with ExecuteRequest {

  import CqlProtocolHelper._

  def serialize(): ByteString = {
    val headerBytes: Array[Byte] = header.serialize()
    val bs = ByteString.newBuilder

    bs.putShort(4)
    bs.putInt(id)

    bs.putShort(consistency.code)
    bs.putByte(flags)

    bs.putShort(variables.size)

    if (variables.size != variableTypes.size) throw new IllegalArgumentException("Must include variable type for every variable")

    variableTypes zip variables foreach  {
      case (varType, varValue) =>
        bs.putBytes(varType.writeValueWithLength(varValue)(VersionTwo))
    }

    val body = bs.result()
    ByteString(headerBytes ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}

case class ExecuteRequestV1(protocolVersion: Byte, stream: Byte, id: Int, consistency : Consistency = ONE, numberOfVariables : Int = 0, variables : List[Any] = List()) extends Request(new Header(protocolVersion, OpCodes.Execute, stream)) with ExecuteRequest {

  import CqlProtocolHelper._

  def serialize(): ByteString = {
    val headerBytes: Array[Byte] = header.serialize()
    val bs = ByteString.newBuilder
    bs.putShort(4)
    bs.putInt(id)
    bs.putShort(0) // 0 variables
    bs.putShort(consistency.code)
    val body = bs.result()
    ByteString(headerBytes ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}
