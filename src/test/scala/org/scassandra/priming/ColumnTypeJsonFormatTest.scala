package org.scassandra.priming

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.priming.PrimingJsonImplicits.ColumnTypeJsonFormat
import spray.json.JsString
import org.scassandra.cqlmessages._
import org.scassandra.cqlmessages.types._
import org.scassandra.cqlmessages.types.CqlSet

class ColumnTypeJsonFormatTest extends FunSuite with Matchers {
  test("Primitive types parsing") {
    ColumnTypeJsonFormat.read(JsString("varchar")) should equal(CqlVarchar)
    ColumnTypeJsonFormat.read(JsString("text")) should equal(CqlText)
    ColumnTypeJsonFormat.read(JsString("ascii")) should equal(CqlAscii)
  }

  test("Set types parsing") {
    ColumnTypeJsonFormat.read(JsString("set")) should equal(CqlSet(CqlVarchar))
    ColumnTypeJsonFormat.read(JsString("set<varchar>")) should equal(CqlSet(CqlVarchar))
    ColumnTypeJsonFormat.read(JsString("set<ascii>")) should equal(CqlSet(CqlAscii))
    ColumnTypeJsonFormat.read(JsString("set<text>")) should equal(CqlSet(CqlText))
  }

  test("List types parsing") {
    ColumnTypeJsonFormat.read(JsString("list")) should equal(CqlList(CqlVarchar))
    ColumnTypeJsonFormat.read(JsString("list<varchar>")) should equal(CqlList(CqlVarchar))
    ColumnTypeJsonFormat.read(JsString("list<ascii>")) should equal(CqlList(CqlAscii))
    ColumnTypeJsonFormat.read(JsString("list<text>")) should equal(CqlList(CqlText))
  }
}
