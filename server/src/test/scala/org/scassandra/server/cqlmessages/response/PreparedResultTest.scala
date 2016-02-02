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
package org.scassandra.server.cqlmessages.response

import org.scassandra.server.cqlmessages.{VersionTwo, OpCodes}
import org.scalatest.{Matchers, FunSuite}
import org.scassandra.server.cqlmessages.CqlProtocolHelper._
import org.scassandra.server.cqlmessages.types.{ColumnType, CqlVarint}

class PreparedResultTest extends FunSuite with Matchers {

  implicit val protocolVersion = VersionTwo
  val stream : Byte = 1

  test("Serialisation of a Prepared result response - V1") {
    val stream: Byte = 0x02
    val preparedStatementId : Byte = 5
    val keyspace : String = "keyspace"
    val table : String = "table"
    val variableTypes = List(CqlVarint)
    val preparedResult = PreparedResultV1(stream, preparedStatementId, keyspace, table, variableTypes)
    val bytes = preparedResult.serialize().iterator
    val length : Byte = 4

    bytes.getByte should equal(protocolVersion.serverCode)
    bytes.getByte should equal(0x0)
    bytes.getByte should equal(stream)
    bytes.getByte should equal(OpCodes.Result)
    val actualLength = bytes.getInt
    bytes.getInt should equal(ResultKinds.Prepared)
    readShortBytes(bytes) should equal(Array[Byte](0,0,0,preparedStatementId))

    // flags - global key space spec
    bytes.getInt should equal(1)
    // column count
    bytes.getInt should equal(1)

    //global spec
    val actualKeyspace = readString(bytes)
    actualKeyspace should equal(keyspace)
    val actualTable = readString(bytes)
    actualTable should equal(table)

    val rowName = readString(bytes)
    rowName should equal("0")

    val rowType = bytes.getShort
    rowType should equal(CqlVarint.code)

    bytes.isEmpty should equal(true)

  }

  test("Serialisation of a Prepared result response - V2") {
    val stream: Byte = 0x02
    val preparedStatementId : Byte = 5
    val keyspace : String = "keyspace"
    val table : String = "table"
    val variableTypes = List(CqlVarint)
    val columns = Map[String, ColumnType[_]]()
    val preparedResult = PreparedResultV2(stream, preparedStatementId, keyspace, table, variableTypes, columns)
    val bytes = preparedResult.serialize().iterator

    bytes.getByte should equal(protocolVersion.serverCode)
    bytes.getByte should equal(0x0)
    bytes.getByte should equal(stream)
    bytes.getByte should equal(OpCodes.Result)
    val actualLength = bytes.getInt
    bytes.getInt should equal(ResultKinds.Prepared)
    readShortBytes(bytes) should equal(Array[Byte](0,0,0,preparedStatementId))

    // flags - global key space spec
    bytes.getInt should equal(1)
    // column count
    bytes.getInt should equal(1)

    //global spec
    val actualKeyspace = readString(bytes)
    actualKeyspace should equal(keyspace)
    val actualTable = readString(bytes)
    actualTable should equal(table)

    val rowName = readString(bytes)
    rowName should equal("0")

    val rowType = bytes.getShort
    rowType should equal(CqlVarint.code)

    // flags
    bytes.getInt should equal(RowsFlags.HasNoMetaData)
    // column count
    bytes.getInt should equal(0)

    bytes.isEmpty should equal(true)

  }


}
