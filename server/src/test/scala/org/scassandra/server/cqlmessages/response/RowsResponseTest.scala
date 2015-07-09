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

import org.scalatest.FunSuite
import org.scalatest.Matchers
import akka.util.ByteString
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.VersionOne
import org.scassandra.server.cqlmessages.types.{CqlVarchar, ColumnType}

import CqlProtocolHelper._

class RowsResponseTest extends FunSuite with Matchers {

  val defaultStreamId : Byte = 1
  implicit val protocolVersion  = VersionOne
  val defaultColumnNames = Map[String, ColumnType[_]]()

  test("Rows message should have Result opcode") {
    val rows = Rows("","", defaultStreamId, defaultColumnNames)
    rows.header.opCode should equal(OpCodes.Result)
  }

  test("Rows message should have result type 0x0002") {
    val rows = Rows("","", defaultStreamId, defaultColumnNames)
    rows.resultKind should equal(ResultKinds.Rows)
  }

  test("Rows serialisation with no rows") {
    val keyspaceName = "keyspace"
    val tableName = "table"
    val stream : Byte = 2
    val rows = Rows(keyspaceName,tableName, stream, defaultColumnNames)

    val body = List(0x0,0x0, 0x0, 0x2, // 4 byte integer for the type of result, 2 is rows
      0x0, 0x0, 0x0, 0x1, // Meta-data - flays saying global table spec
      0x0, 0x0, 0x0, defaultColumnNames.size.toByte, // col count
      0x0, keyspaceName.length.toByte) :::
      keyspaceName.getBytes().toList :::
      List[Byte](0x0, tableName.length.toByte) :::
      tableName.getBytes().toList :::
      List[Byte](0x0, 0x0, 0x0, 0x0) // row count

    val result = List[Byte](
      protocolVersion.serverCode, // protocol version
      0x00, // flags
      stream, // stream
      0x08, // message type - 8 (Result)
      0x0, 0x0, 0x0, body.length.toByte) ::: // length - int
      body

    rows.serialize() should equal(result)
  }

  test("Serialisation of a Rows result") {
    val keyspaceName = "someKeyspace"
    val tableName = "users"
    val columnNames = Map("name" -> CqlVarchar, "age" -> CqlVarchar)

    val rowsToSerialise: List[Row] =
      List(
        Row(Map(
          "name" -> "Mickey",
          "age" -> "23"
        )),
        Row(Map(
          "name" -> "Mario",
          "age" -> "74"
        ))
      )
    val stream: Byte = 0x01
    val rows = Rows(keyspaceName, tableName, stream, columnNames, rowsToSerialise)
    val actualBytes = rows.serialize().toList

    val expectedBody = List[Byte](
      0x0, 0x0, 0x0, 0x2, // 4 byte integer for the type of result, 2 is a "rows" result
      0x0, 0x0, 0x0, 0x1, // 4 byte integer for the global_table_spec flag
      0x0, 0x0, 0x0, 0x2 // 4 byte integer for the column count
    ) :::
      // global_table_spec
      (toNativeProtocolString(keyspaceName)) :::
      (toNativeProtocolString(tableName)) :::
      // col_spec_1, Varchar
      (toNativeProtocolString("name")) :::
      (List[Byte](0x00, 0x0D)) :::
      // col_spec_2, Varchar
      (toNativeProtocolString("age")) :::
      (List[Byte](0x00, 0x0D)) :::
      // rows count
      (List[Byte](0x0, 0x0, 0x0, 0x2)) ::: // 4 byte integer for the rows count
      // first row
      // first column value: Mickey as [bytes]
      (List[Byte](0x0, 0x0, 0x0, 0x6)) ::: // 4 byte integer for the length
      ("Mickey".getBytes.toList) :::
      // second column value: 23 as [bytes]
      (List[Byte](0x0, 0x0, 0x0, 0x2)) ::: // 4 byte integer for the length
      ("23".getBytes.toList) :::
      // second row
      // first column value: Mario as [bytes]
      (List[Byte](0x0, 0x0, 0x0, 0x5)) ::: // 4 byte integer for the length
      ("Mario".getBytes.toList) :::
      // second column value: 74 as [bytes]
      (List[Byte](0x0, 0x0, 0x0, 0x2)) ::: // 4 byte integer for the length
      ("74".getBytes.toList)

    val expectedHeader = List[Byte](
      protocolVersion.serverCode, // protocol version
      0x00, // flags
      stream, // stream
      0x08 // message type - 8 (Result)
    ) ::: serializeInt(expectedBody.length) // 4 byte integer - length of body (number of bytes)

    actualBytes should equal(
      expectedHeader ::: expectedBody
    )
  }


  def toNativeProtocolString(string: String): List[Byte] = {
    // [string] = [short] + n bytes
    List[Byte](
      // Note: this assumes that the string length fits in one byte == max 127 chars
      0x0, string.length.toByte
    ) ::: string.getBytes.toList
  }

  def serializeInt(int: Int): List[Byte] = {

    val builder = ByteString.newBuilder
    builder.putInt(int)
    var result = builder.result().toList
    while (result.length < 4) {
      result = 0x00.toByte :: result
    }
    result
  }

}

