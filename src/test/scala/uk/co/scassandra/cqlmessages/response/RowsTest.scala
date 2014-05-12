package uk.co.scassandra.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}
import akka.util.ByteIterator
import uk.co.scassandra.cqlmessages._
import java.util.UUID
import java.net.InetAddress
import uk.co.scassandra.cqlmessages.{CqlVarchar, ColumnType, VersionTwo}

class RowsTest extends FunSuite with Matchers {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  implicit val protocolVersion = VersionTwo

  test("Serialization of Varchar column type") {
    val columnNames: Map[String, ColumnType] = Map("age" -> CqlVarchar)
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
    val columnNames: Map[String, ColumnType] = Map("age" -> CqlText)
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
    val columnNames: Map[String, ColumnType] = Map("age" -> CqlInt)
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
    val columnNames: Map[String, ColumnType] = Map("age" -> CqlInt)
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
    val columnNames: Map[String, ColumnType] = Map("age" -> CqlInt)
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
    val columnNames: Map[String, ColumnType] = Map("booleanValue" -> CqlBoolean)
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
    val columnNames: Map[String, ColumnType] = Map("name" -> CqlAscii)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlBigint)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlBigint)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlCounter)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlBlob)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlDecimal)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlDouble)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlFloat)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlTimestamp)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlUUID)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlTimeUUID)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlInet)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlVarint)
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
    val columnNames: Map[String, ColumnType] = Map("field" -> CqlSet)
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

    val rowValue = CqlProtocolHelper.readVarcharSet(rowsBytes)
    rowValue should equal(varcharSet)
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
