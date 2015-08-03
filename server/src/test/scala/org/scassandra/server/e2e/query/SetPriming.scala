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
package org.scassandra.server.e2e.query

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.datastax.driver.core.DataType
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

class SetPriming extends AbstractIntegrationTest {

  test("Test a set of varchars") {
    val set = Set("one", "two", "three")
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlVarchar))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.varchar()))

    val expectedSet = new util.HashSet(util.Arrays.asList("one","two","three"))
    singleRow.getSet("field", Class.forName("java.lang.String")) should equal(expectedSet)
  }

  test("Test a set of ascii") {
    val set = Set("one", "two", "three")
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlAscii))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.ascii()))

    val expectedSet = new util.HashSet(util.Arrays.asList("one","two","three"))
    singleRow.getSet("field", Class.forName("java.lang.String")) should equal(expectedSet)
  }

  test("Test a set of text") {
    val set = Set("one", "two", "three")
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlText))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.text()))

    val expectedSet = new util.HashSet(util.Arrays.asList("one","two","three"))
    singleRow.getSet("field", Class.forName("java.lang.String")) should equal(expectedSet)
  }

  test("Test a set of int") {
    val set = Set(1, 2, 3)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlInt))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.cint()))

    val expectedSet = new util.HashSet(util.Arrays.asList(1,2,3))
    singleRow.getSet("field", Class.forName("java.lang.Integer")) should equal(expectedSet)
  }

  test("Test a set of bigint (java long)") {
    val set = Set(1l, 2l, 3l)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlBigint))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.bigint()))

    val expectedSet = new util.HashSet(util.Arrays.asList(1l,2l,3l))
    singleRow.getSet("field", Class.forName("java.lang.Long")) should equal(expectedSet)
  }

  test("Test a set of boolean") {
    val set = Set(true, false, true)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlBoolean))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.cboolean()))

    val expectedSet = new util.HashSet(util.Arrays.asList(true, false, true))
    singleRow.getSet("field", Class.forName("java.lang.Boolean")) should equal(expectedSet)
  }

  test("Test a set of counter") {
    val set = Set(1l, 2l, 3l)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlCounter))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.counter()))

    val expectedSet = new util.HashSet(util.Arrays.asList(1l, 2l, 3l))
    singleRow.getSet("field", Class.forName("java.lang.Long")) should equal(expectedSet)
  }

  test("Test a set of decimal") {
    val set = Set(BigDecimal("1.2"), BigDecimal("2.3"), BigDecimal("3.4"))
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlDecimal))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.decimal()))

    val expectedSet = new util.HashSet(util.Arrays.asList(new java.math.BigDecimal("1.2"), new java.math.BigDecimal("2.3"), new java.math.BigDecimal("3.4")))
    singleRow.getSet("field", Class.forName("java.math.BigDecimal")) should equal(expectedSet)
  }

  test("Test a set of double") {
    val set = Set(1.0, 2.0, 3.0)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlDouble))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.cdouble()))

    val expectedSet = new util.HashSet(util.Arrays.asList(1.0, 2.0, 3.0))
    singleRow.getSet("field", Class.forName("java.lang.Double")) should equal(expectedSet)
  }

  test("Test a set of float") {
    val set = Set(1.0f, 2.0f, 3.0f)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlFloat))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.cfloat()))

    val expectedSet = new util.HashSet(util.Arrays.asList(1.0f, 2.0f, 3.0f))
    singleRow.getSet("field", Class.forName("java.lang.Float")) should equal(expectedSet)
  }

  test("Test a set of inet") {
    val set = Set(InetAddress.getLocalHost)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlInet))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.inet()))

    val expectedSet = new util.HashSet(util.Arrays.asList(InetAddress.getLocalHost))
    singleRow.getSet("field", Class.forName("java.net.InetAddress")) should equal(expectedSet)
  }

  test("Test a set of date") {
    val date = new Date
    val set = Set(date.getTime)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlTimestamp))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.timestamp()))

    val expectedSet = new util.HashSet(util.Arrays.asList(date))
    singleRow.getSet("field", Class.forName("java.util.Date")) should equal(expectedSet)
  }

  test("Test a set of uuid") {
    val uuid = UUID.randomUUID()
    val set = Set(uuid)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlUUID))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.uuid()))

    val expectedSet = new util.HashSet(util.Arrays.asList(uuid))
    singleRow.getSet("field", Class.forName("java.util.UUID")) should equal(expectedSet)
  }

  test("Test a set of timeuuid") {
    val uuid = UUID.fromString("1c0e8c70-754b-11e4-ac06-4b05b98cc84c")
    val set = Set(uuid)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlTimeUUID))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.timeuuid()))

    val expectedSet = new util.HashSet(util.Arrays.asList(uuid))
    singleRow.getSet("field", Class.forName("java.util.UUID")) should equal(expectedSet)
  }

  test("Test a set of varint") {
    val set = Set(BigInt("1"), BigInt("2"), BigInt("3"))
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlVarint))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.varint()))

    val expectedSet = new util.HashSet(util.Arrays.asList(new BigInteger("1"), new BigInteger("2"), new BigInteger("3")))
    singleRow.getSet("field", Class.forName("java.math.BigInteger")) should equal(expectedSet)
  }

  test("Test a set of blob") {
    val blob = Array[Byte](1,2,3,4,5)
    val set = Set(blob, blob)
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val columnTypes  = Map("field" -> CqlSet(CqlBlob))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.set(DataType.blob()))

    val expectedSet = new util.HashSet(util.Arrays.asList(ByteBuffer.wrap(blob), ByteBuffer.wrap(blob)))
    singleRow.getSet("field", Class.forName("java.nio.ByteBuffer")) should equal(expectedSet)
  }
}