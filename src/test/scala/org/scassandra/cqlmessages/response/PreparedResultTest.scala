package org.scassandra.cqlmessages.response

import org.scassandra.cqlmessages.{VersionTwo, CqlProtocolHelper, OpCodes, CqlVarint}
import org.scalatest.{Matchers, FunSuite}

class PreparedResultTest extends FunSuite with Matchers {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
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
    CqlProtocolHelper.readShortBytes(bytes) should equal(Array[Byte](0,0,0,preparedStatementId))

    // flags - global key space spec
    bytes.getInt should equal(1)
    // column count
    bytes.getInt should equal(1)

    //global spec
    val actualKeyspace = CqlProtocolHelper.readString(bytes)
    actualKeyspace should equal(keyspace)
    val actualTable = CqlProtocolHelper.readString(bytes)
    actualTable should equal(table)

    val rowName = CqlProtocolHelper.readString(bytes)
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
    val preparedResult = PreparedResultV2(stream, preparedStatementId, keyspace, table, variableTypes)
    val bytes = preparedResult.serialize().iterator

    bytes.getByte should equal(protocolVersion.serverCode)
    bytes.getByte should equal(0x0)
    bytes.getByte should equal(stream)
    bytes.getByte should equal(OpCodes.Result)
    val actualLength = bytes.getInt
    bytes.getInt should equal(ResultKinds.Prepared)
    CqlProtocolHelper.readShortBytes(bytes) should equal(Array[Byte](0,0,0,preparedStatementId))

    // flags - global key space spec
    bytes.getInt should equal(1)
    // column count
    bytes.getInt should equal(1)

    //global spec
    val actualKeyspace = CqlProtocolHelper.readString(bytes)
    actualKeyspace should equal(keyspace)
    val actualTable = CqlProtocolHelper.readString(bytes)
    actualTable should equal(table)

    val rowName = CqlProtocolHelper.readString(bytes)
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
