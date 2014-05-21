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
package org.scassandra.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteIterator
import org.scassandra.cqlmessages._
import java.util.UUID
import java.net.InetAddress
import org.scassandra.cqlmessages.{CqlVarchar, VersionTwo}

class RowsTest extends FunSuite with Matchers {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  implicit val protocolVersion = VersionTwo

  test("Serialization of Varchar column type") {
    val columnNames = Map("age" -> CqlVarchar)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("age")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlVarchar.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readLongString(rowsBytes)
    rowValue should equal("18")
  }

  test("Serialization of Text column type") {
    val columnNames = Map("age" -> CqlText)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("age")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlText.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readLongString(rowsBytes)
    rowValue should equal("18")
  }

  test("Serialization of Int column type where value is a Scala Int") {
    val columnNames = Map("age" -> CqlInt)
    val rows: List[Row] = List(Row(Map("age" -> 18)))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("age")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlInt.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readIntValue(rowsBytes)
    rowValue should equal(18)
  }

  test("Serialization of Int column type where value is a Scala Long") {
    val columnNames = Map("age" -> CqlInt)
    val rows: List[Row] = List(Row(Map("age" -> 18l)))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("age")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlInt.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readIntValue(rowsBytes)
    rowValue should equal(18)
  }

  test("Serialization of Int column type where value is a Scala String") {
    val columnNames = Map("age" -> CqlInt)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("age")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlInt.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readIntValue(rowsBytes)
    rowValue should equal(18)
  }

  test("Serialization of Boolean column type where value is false") {
    val columnNames = Map("booleanValue" -> CqlBoolean)
    val rows: List[Row] = List(Row(Map("booleanValue" -> "false")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("booleanValue")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlBoolean.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readBooleanValue(rowsBytes)
    rowValue should equal(false)
  }

  test("Serialization of Ascii column type") {
    val columnNames = Map("name" -> CqlAscii)
    val rows: List[Row] = List(Row(Map("name" -> "Chris Batey")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("name")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlAscii.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readLongString(rowsBytes)
    rowValue should equal("Chris Batey")
  }

  test("Serialization of BigInt column type when value is a String") {
    val columnNames = Map("field" -> CqlBigint)
    val rows: List[Row] = List(Row(Map("field" -> "1234")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlBigint.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readBigIntValue(rowsBytes)
    rowValue should equal(1234)
  }

  test("Serialization of BigInt column type when value is a Long") {
    val columnNames = Map("field" -> CqlBigint)
    val rows: List[Row] = List(Row(Map("field" -> 1234)))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlBigint.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readBigIntValue(rowsBytes)
    rowValue should equal(1234)
  }

  test("Serialization of Counter column type when value is a String") {
    val columnNames = Map("field" -> CqlCounter)
    val rows: List[Row] = List(Row(Map("field" -> "1234")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlCounter.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readBigIntValue(rowsBytes)
    rowValue should equal(1234)
  }

  test("Serialization of Blob column type") {
    val columnNames = Map("field" -> CqlBlob)
    val rows: List[Row] = List(Row(Map("field" -> "0x48656c6c6f")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlBlob.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readBlobValue(rowsBytes)
    rowValue should equal(Array[Byte](0x48, 0x65, 0x6c, 0x6c, 0x6f))
  }

  test("Serialization of Decimal column type") {
    val columnNames = Map("field" -> CqlDecimal)
    val rows: List[Row] = List(Row(Map("field" -> "5.5456")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlDecimal.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readDecimalValue(rowsBytes)
    rowValue should equal(BigDecimal("5.5456"))
  }

  test("Serialization of Double column type") {
    val columnNames = Map("field" -> CqlDouble)
    val rows: List[Row] = List(Row(Map("field" -> "5.5456")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlDouble.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readDoubleValue(rowsBytes)
    rowValue should equal(5.5456)
  }

  test("Serialization of Float column type") {
    val columnNames = Map("field" -> CqlFloat)
    val rows: List[Row] = List(Row(Map("field" -> "5.5456")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlFloat.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readFloatValue(rowsBytes)
    rowValue should equal(5.5456f)
  }

  test("Serialization of Timestamp column type") {
    val columnNames = Map("field" -> CqlTimestamp)
    val rows: List[Row] = List(Row(Map("field" -> "1368438171000")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlTimestamp.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readTimestampValue(rowsBytes)
    rowValue should equal(1368438171000l)
  }

  test("Serialization of UUID column type") {
    val uuid = UUID.randomUUID()
    val columnNames = Map("field" -> CqlUUID)
    val rows: List[Row] = List(Row(Map("field" -> uuid.toString)))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlUUID.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readUUIDValue(rowsBytes)
    rowValue should equal(uuid)
  }

  test("Serialization of TimeUUID column type") {
    val uuid = UUID.fromString("2c530380-b9f9-11e3-850e-338bb2a2e74f")
    val columnNames = Map("field" -> CqlTimeUUID)
    val rows: List[Row] = List(Row(Map("field" -> uuid.toString)))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlTimeUUID.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readUUIDValue(rowsBytes)
    rowValue should equal(uuid)
  }

  test("Serialization of Inet column type") {
    val inet = InetAddress.getByAddress(Array[Byte](127,0,0,1))
    val columnNames = Map("field" -> CqlInet)
    val rows: List[Row] = List(Row(Map("field" -> "127.0.0.1")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlInet.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readInetValue(rowsBytes)
    rowValue should equal(inet)
  }

  test("Serialization of Varint column type") {
    val varint = BigInt("1234")
    val columnNames = Map("field" -> CqlVarint)
    val rows: List[Row] = List(Row(Map("field" -> "1234")))
    val rowsBytes = Rows("keyspaceName","tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlVarint.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readVarintValue(rowsBytes)
    rowValue should equal(varint)
  }
 test("Serialization of Set of varchars column type") {
    val varcharSet = Set("one","two")
    val columnNames = Map("field" -> CqlSet)
    val rows: List[Row] = List(Row(Map("field" -> varcharSet)))
    val rowsBytes = Rows("keyspaceName", "tableName", 1, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(1)

    dropKeyspaceAndTableName(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(CqlSet.code)

    val setType = rowsBytes.getShort
    setType should equal(CqlVarchar.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readVarcharSetValue(rowsBytes)
    rowValue should equal(varcharSet)
  }

  test("Serialisation of multiple rows where some columns are null") {
    val columnNames = Map(
      "id" -> CqlInt,
      "col_blob" -> CqlBlob,
      "col_one" -> CqlVarchar,
      "col_two" -> CqlInt,
      "col_three" -> CqlTimeUUID)
    val rows: List[Row] = List(
      Row(Map("id" -> 1, "col_one" -> "hello", "col_three" -> "f535e350-e111-11e3-baa7-070c076eda0a", "col_two" -> 2)),
      Row(Map("id" -> 2, "col_three" -> "1bd8f0b0-e112-11e3-baa7-070c076eda0a")),
      Row(Map("id" -> 3, "col_two" -> "2")),
      Row(Map("id" -> 4, "col_blob" -> "0x12"))
    )
    val rowsBytes = Rows("people", "various_columns", 0, columnNames, rows).serialize().iterator

    dropHeaderAndLength(rowsBytes)
    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(5)
    dropKeyspaceAndTableName(rowsBytes)

    var rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("col_one")
    var rowType = rowsBytes.getShort
    rowType should equal(CqlVarchar.code)

    rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("col_two")
    rowType = rowsBytes.getShort
    rowType should equal(CqlInt.code)

    rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("col_blob")
    rowType = rowsBytes.getShort
    rowType should equal(CqlBlob.code)

    rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("id")
    rowType = rowsBytes.getShort
    rowType should equal(CqlInt.code)

    rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("col_three")
    rowType = rowsBytes.getShort
    rowType should equal(CqlTimeUUID.code)

    val rowCount = rowsBytes.getInt
    rowCount should equal(4)

    // first row
    // col - col_one, string, "hello"
    val rowOneColOne = CqlProtocolHelper.readLongString(rowsBytes)
    rowOneColOne should equal("hello")

    // col - col_two, int, 2
    val rowOneColTwo = CqlProtocolHelper.readIntValue(rowsBytes)
    rowOneColTwo should equal(2)

    // col - col_blob, blob, null
    val rowOneColBlob = CqlProtocolHelper.readNullValue(rowsBytes)
    rowOneColBlob should equal(true)
  }

  def dropKeyspaceAndTableName(rowsBytes: ByteIterator) {
    // keyspace name
    CqlProtocolHelper.readString(rowsBytes)
    // table name
    CqlProtocolHelper.readString(rowsBytes)
  }

  def dropHeaderAndLength(rowsBytes: ByteIterator) {
    // drop header
    rowsBytes.drop(4)
    //drop length
    rowsBytes.drop(4)
    //result kind
    rowsBytes.drop(4)
    //flags
    rowsBytes.drop(4)
  }
}

/*
[-126, 0, 0, 8,
0, 0, 0, -20,
0, 0, 0, 2,
0, 0, 0, 1, // flags
0, 0, 0, 5, // col count
0, 6,
112, 101, 111, 112, 108, 101,
0, 15,
118, 97, 114, 105, 111, 117, 115, 95, 99, 111, 108, 117, 109, 110, 115,
0, 2,   105, 100,   // col name
0, 9, // col type - int
0, 8,   99, 111, 108, 95, 98, 108, 111, 98,
0, 3, // col type blob
0, 7,   99, 111, 108, 95, 111, 110, 101,
0, 13,
0, 9,   99, 111, 108, 95, 116, 104, 114, 101, 101,
0, 15,
0, 7,    99, 111, 108, 95, 116, 119, 111,
0, 9,

0, 0, 0, 4, // row count
0, 0, 0, 4,    0, 0, 0, 1,
-1, -1, -1, -1,
0, 0, 0, 5,    104, 101, 108, 108, 111,
0, 0, 0, 16,   -11, 53, -29, 80, -31, 17, 17, -29, -70, -89, 7, 12, 7, 110, -38, 10,
0, 0, 0, 4,   0, 0, 0, 2,
0, 0, 0, 4, 0, 0, 0, 2,
-1, -1, -1, -1,
-1, -1, -1, -1,
0, 0, 0, 16,   27, -40, -16, -80, -31, 18, 17, -29, -70, -89, 7, 12, 7, 110, -38, 10,
-1, -1, -1, -1,
0, 0, 0, 4,   0, 0, 0, 4,
0, 0, 0, 1,   18,
-1, -1, -1, -1,
-1, -1, -1, -1,
-1, -1, -1, -1,
 0, 0, 0, 4,   0, 0, 0, 3,
 -1, -1, -1, -1,
 -1, -1, -1, -1,
 -1, -1, -1, -1,
 0, 0, 0, 4,   0, 0, 0, 2]
*/