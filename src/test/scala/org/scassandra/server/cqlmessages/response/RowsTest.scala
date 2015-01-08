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

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteIterator
import java.util.UUID
import java.net.InetAddress
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.types._

class RowsTest extends FunSuite with Matchers {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  implicit val protocolVersion = VersionTwo
  val stream : Byte = 1
  
  test("Serialisation of number of columns") {
    val columnTypes = Map("age" -> CqlVarchar, "name" -> CqlVarchar)
    val rowWithTwoColumns: List[Row] = List(Row(Map("age" -> "18", "name" -> "Chris")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnTypes, rowWithTwoColumns).serialize().iterator

    dropHeaderAndLength(rowsBytes)

    val numberOfColumns = rowsBytes.getInt
    numberOfColumns should equal(2)
  }

  test("Serialisation of number of keyspace and tablename") {
    val columnTypes = Map("age" -> CqlVarchar, "name" -> CqlVarchar)
    val rowWithTwoColumns: List[Row] = List(Row(Map("age" -> "18", "name" -> "Chris")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnTypes, rowWithTwoColumns).serialize().iterator

    dropHeaderAndLengthAndColumnCount(rowsBytes)

    // keyspace name
    CqlProtocolHelper.readString(rowsBytes) should equal("keyspaceName")
    // table name
    CqlProtocolHelper.readString(rowsBytes) should equal("tableName")
  }

  test("Serialization of Varchar column type") {
    val columnNames = Map("age" -> CqlVarchar)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator

    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "age", CqlVarchar)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readLongString(rowsBytes).get
    rowValue should equal("18")
  }

  private def dropNumberOfRows(iterator: ByteIterator) = {
    iterator.drop(4)
  }

  test("Serialization of Text column type") {
    val columnNames = Map("age" -> CqlText)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator

    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "age", CqlText)

    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readLongString(rowsBytes).get
    rowValue should equal("18")
  }

  test("Serialization of Int column type where value is a BigDecimal") {
    val columnNames = Map("age" -> CqlInt)
    val rows: List[Row] = List(Row(Map("age" -> BigDecimal(18))))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "age", CqlInt)

    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readIntValue(rowsBytes).get
    rowValue should equal(18)
  }

  test("Serialization of Int column type where value is a Scala string") {
    val columnNames = Map("age" -> CqlInt)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "age", CqlInt)

    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readIntValue(rowsBytes).get
    rowValue should equal(18)
  }

  test("Serialization of Int column type where value is a Scala String") {
    val columnNames = Map("age" -> CqlInt)
    val rows: List[Row] = List(Row(Map("age" -> "18")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "age", CqlInt)

    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readIntValue(rowsBytes).get
    rowValue should equal(18)
  }

  test("Serialization of Boolean column type where value is false") {
    val columnNames = Map("booleanValue" -> CqlBoolean)
    val rows: List[Row] = List(Row(Map("booleanValue" -> "false")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "booleanValue", CqlBoolean)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readBooleanValue(rowsBytes).get
    rowValue should equal(false)
  }

  test("Serialization of Ascii column type") {
    val columnNames = Map("name" -> CqlAscii)
    val rows: List[Row] = List(Row(Map("name" -> "Chris Batey")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "name", CqlAscii)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readLongString(rowsBytes).get
    rowValue should equal("Chris Batey")
  }

  test("Serialization of BigInt column type when value is a String") {
    val columnNames = Map("field" -> CqlBigint)
    val rows: List[Row] = List(Row(Map("field" -> "1234")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlBigint)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readBigIntValue(rowsBytes).get
    rowValue should equal(1234)
  }

  test("Serialization of BigInt column type when value is a Long") {
    val columnNames = Map("field" -> CqlBigint)
    val rows: List[Row] = List(Row(Map("field" -> 1234)))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlBigint)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readBigIntValue(rowsBytes).get
    rowValue should equal(1234)
  }

  test("Serialization of Counter column type when value is a String") {
    val columnNames = Map("field" -> CqlCounter)
    val rows: List[Row] = List(Row(Map("field" -> "1234")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlCounter)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readBigIntValue(rowsBytes).get
    rowValue should equal(1234)
  }

  test("Serialization of Blob column type") {
    val columnNames = Map("field" -> CqlBlob)
    val rows: List[Row] = List(Row(Map("field" -> "0x48656c6c6f")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlBlob)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readBlobValue(rowsBytes).get
    rowValue should equal(Array[Byte](0x48, 0x65, 0x6c, 0x6c, 0x6f))
  }

  test("Serialization of Decimal column type") {
    val columnNames = Map("field" -> CqlDecimal)
    val rows: List[Row] = List(Row(Map("field" -> "5.5456")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlDecimal)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readDecimalValue(rowsBytes).get
    rowValue should equal(BigDecimal("5.5456"))
  }

  test("Serialization of Double column type") {
    val columnNames = Map("field" -> CqlDouble)
    val rows: List[Row] = List(Row(Map("field" -> "5.5456")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlDouble)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readDoubleValue(rowsBytes).get
    rowValue should equal(5.5456)
  }

  test("Serialization of Float column type") {
    val columnNames = Map("field" -> CqlFloat)
    val rows: List[Row] = List(Row(Map("field" -> "5.5456")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlFloat)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readFloatValue(rowsBytes).get
    rowValue should equal(5.5456f)
  }

  test("Serialization of Timestamp column type") {
    val columnNames = Map("field" -> CqlTimestamp)
    val rows: List[Row] = List(Row(Map("field" -> "1368438171000")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlTimestamp)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readTimestampValue(rowsBytes).get
    rowValue should equal(1368438171000l)
  }

  test("Serialization of UUID column type") {
    val uuid = UUID.randomUUID()
    val columnNames = Map("field" -> CqlUUID)
    val rows: List[Row] = List(Row(Map("field" -> uuid.toString)))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlUUID)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readUUIDValue(rowsBytes).get
    rowValue should equal(uuid)
  }

  test("Serialization of TimeUUID column type") {
    val uuid = UUID.fromString("2c530380-b9f9-11e3-850e-338bb2a2e74f")
    val columnNames = Map("field" -> CqlTimeUUID)
    val rows: List[Row] = List(Row(Map("field" -> uuid.toString)))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlTimeUUID)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readUUIDValue(rowsBytes).get
    rowValue should equal(uuid)
  }

  test("Serialization of Inet column type") {
    val inet = InetAddress.getByAddress(Array[Byte](127,0,0,1))
    val columnNames = Map("field" -> CqlInet)
    val rows: List[Row] = List(Row(Map("field" -> "127.0.0.1")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlInet)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readInetValue(rowsBytes).get
    rowValue should equal(inet)
  }

  test("Serialization of Varint column type") {
    val varint = BigInt("1234")
    val columnNames = Map("field" -> CqlVarint)
    val rows: List[Row] = List(Row(Map("field" -> "1234")))
    val rowsBytes = Rows("keyspaceName","tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    verifyPrimitiveRowTypeMetadata(rowsBytes, "field", CqlVarint)
    dropNumberOfRows(rowsBytes)

    val rowValue = CqlProtocolHelper.readVarintValue(rowsBytes).get
    rowValue should equal(varint)
  }

 test("Serialization of Set of varchars column type") {
    val varcharSet = Set("one","two")
    val setOfVarcharType: CqlSet[String] = CqlSet(CqlVarchar)
    val columnNames = Map("field" -> setOfVarcharType)
    val rows: List[Row] = List(Row(Map("field" -> varcharSet)))
    val rowsBytes = Rows("keyspaceName", "tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(setOfVarcharType.code)

    val setType = rowsBytes.getShort
    setType should equal(CqlVarchar.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readVarcharSetValue(rowsBytes).get
    rowValue should equal(varcharSet)
  }

  test("Serialization of List of varchars column type") {
    val varcharSet = Set("one","two")
    val listOfVarcharType = CqlList(CqlVarchar)
    val columnNames = Map("field" -> listOfVarcharType)
    val rows: List[Row] = List(Row(Map("field" -> varcharSet)))
    val rowsBytes = Rows("keyspaceName", "tableName", stream, columnNames, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

    val rowName = CqlProtocolHelper.readString(rowsBytes)
    rowName should equal("field")

    val rowType = rowsBytes.getShort
    rowType should equal(listOfVarcharType.code)

    val listType = rowsBytes.getShort
    listType should equal(CqlVarchar.code)

    val numberOfRows = rowsBytes.getInt
    numberOfRows should equal(1)

    val rowValue = CqlProtocolHelper.readVarcharSetValue(rowsBytes).get
    rowValue should equal(varcharSet)
  }

  test("Serialisation of multiple rows where some columns are null") {
    val columnTypes = Map[String, ColumnType[_]](
      "id" -> CqlInt,
      "col_blob" -> CqlBlob,
      "col_one" -> CqlVarchar,
      "col_two" -> CqlInt,
      "col_three" -> CqlTimeUUID)

    val rows: List[Row] = List(
      Row(Map("id" -> "1", "col_one" -> "hello", "col_three" -> "f535e350-e111-11e3-baa7-070c076eda0a", "col_two" -> BigDecimal(2))),
      Row(Map("id" -> "2", "col_three" -> "1bd8f0b0-e112-11e3-baa7-070c076eda0a")),
      Row(Map("id" -> "3", "col_two" -> "2")),
      Row(Map("id" -> "4", "col_blob" -> "0x12"))
    )
    val rowsBytes = Rows("people", "various_columns", stream, columnTypes, rows).serialize().iterator
    dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes)

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
    val rowOneColOne = CqlProtocolHelper.readLongString(rowsBytes).get
    rowOneColOne should equal("hello")

    // col - col_two, int, 2
    val rowOneColTwo = CqlProtocolHelper.readIntValue(rowsBytes).get
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
  
  def dropHeaderAndLengthAndColumnCountAndKeyspaceAndTable(rowsBytes: ByteIterator) {
    dropHeaderAndLengthAndColumnCount(rowsBytes)
    dropKeyspaceAndTableName(rowsBytes)
  }

  def dropHeaderAndLengthAndColumnCount(rowsBytes : ByteIterator) {
    dropHeaderAndLength(rowsBytes)
    rowsBytes.drop(4) // col count
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


  private def verifyPrimitiveRowTypeMetadata(rowBytes: ByteIterator, name: String, columnType: ColumnType[_]) {
    val rowName = CqlProtocolHelper.readString(rowBytes)
    rowName should equal(name)

    val rowType = rowBytes.getShort
    rowType should equal(columnType.code)
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