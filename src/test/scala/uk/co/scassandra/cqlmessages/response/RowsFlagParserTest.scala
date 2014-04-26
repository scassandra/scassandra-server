package uk.co.scassandra.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}

class RowsFlagParserTest extends FunSuite with Matchers {
  test("Test") {
    println(RowsFlags.GlobalTableSpec)
    println(RowsFlags.HasMorePages)
    println(RowsFlags.HasNoMetaData)

  }

  test("1 should have GlobalTableSpec and not MorePages or NoMetadata") {
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, 1) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, 1) should equal(false)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, 1) should equal(false)
  }

  test("2 should have MorePages and not GlobalTableSpec or NoMetadata") {
    val value = 2;
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, value) should equal(false)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, value) should equal(false)
  }

  test("3 should have MorePages and GlobalTableSpec but not NoMetadata") {
    val value = 3;
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, value) should equal(false)
  }

  test("7 should have MorePages and GlobalTableSpec and NoMetadata") {
    val value = 7;
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, value) should equal(true)
  }
}
