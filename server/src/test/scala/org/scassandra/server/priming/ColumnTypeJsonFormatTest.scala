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
package org.scassandra.server.priming

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.server.priming.json.PrimingJsonImplicits
import PrimingJsonImplicits.ColumnTypeJsonFormat
import spray.json.JsString
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.cqlmessages.types.CqlSet

class ColumnTypeJsonFormatTest extends FunSuite with Matchers {
  test("Primitive types parsing") {
    ColumnTypeJsonFormat.read(JsString("varchar")) should equal(CqlVarchar)
    ColumnTypeJsonFormat.read(JsString("text")) should equal(CqlText)
    ColumnTypeJsonFormat.read(JsString("ascii")) should equal(CqlAscii)
  }

  test("Set types parsing") {
    ColumnTypeJsonFormat.read(JsString("set<varchar>")) should equal(CqlSet(CqlVarchar))
    ColumnTypeJsonFormat.read(JsString("set<ascii>")) should equal(CqlSet(CqlAscii))
    ColumnTypeJsonFormat.read(JsString("set<text>")) should equal(CqlSet(CqlText))
  }

  test("List types parsing") {
    ColumnTypeJsonFormat.read(JsString("list<varchar>")) should equal(CqlList(CqlVarchar))
    ColumnTypeJsonFormat.read(JsString("list<ascii>")) should equal(CqlList(CqlAscii))
    ColumnTypeJsonFormat.read(JsString("list<text>")) should equal(CqlList(CqlText))
  }
}
