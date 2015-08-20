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
package org.scassandra.server.cqlmessages

import scala.language.postfixOps

import akka.util.{ByteIterator, ByteString}

import org.scassandra.server.cqlmessages.CqlProtocolHelper._
import org.scassandra.server.cqlmessages.request.ExecuteRequest
import org.scassandra.server.cqlmessages.response._
import org.scassandra.server.cqlmessages.types.ColumnType


object VersionTwoMessageFactory extends AbstractMessageFactory {

  implicit val protocolVersion = VersionTwo

  def createPreparedResult(stream: Byte, id : Int, variableTypes: List[ColumnType[_]]) = {
    PreparedResultV2(stream, id, "keyspace", "table", variableTypes)
  }

  def parseExecuteRequestWithoutVariables(stream: Byte, byteString: ByteString): ExecuteRequest = {
    ExecuteRequest.versionTwoWithoutTypes(stream, byteString)
  }

  def parseExecuteRequestWithVariables(stream: Byte, byteString: ByteString, variableTypes: List[ColumnType[_]]): ExecuteRequest = {
    ExecuteRequest.versionTwoWithTypes(stream, byteString, variableTypes)
  }

  def parseBatchQuery(iterator: ByteIterator): String = {
    val query: String = readLongString(iterator).get
    val numVariables = iterator.getShort
    // read off the bytes for each variable, we can't parse them until priming of batches is supported
    (0 until numVariables).foreach { _ => readLongBytes(iterator) }
    query
  }

  /**
   * Reads a short to determine how many variables there are then reads that many
   * short bytes
   * @param iterator To read from
   * @return A list of variables as raw bytes
   */
  def readVariables(iterator: ByteIterator): List[Array[Byte]] = {
    val numVariables = iterator.getShort
    (0 until numVariables).map { _ => readLongBytes(iterator) } toList
  }
}
