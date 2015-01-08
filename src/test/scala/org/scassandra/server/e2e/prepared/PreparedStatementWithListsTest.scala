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
package org.scassandra.server.e2e.prepared

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{UUID, Date}

import com.datastax.driver.core.Row
import dispatch.Defaults._
import dispatch._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}

class PreparedStatementWithListsTest  extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()

    val svcDeleteActivity = url("http://localhost:8043/prepared-statement-execution").DELETE
    val responseDeleteActivity = Http(svcDeleteActivity OK as.String)
    responseDeleteActivity()
  }

  test("Text list as a varchar list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList("one", "two", "three")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlVarchar))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    val result = session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List("one", "two", "three"))
  }

  test("Text list as a text list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList("one", "two", "three")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlText))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List("one", "two", "three"))
  }

  test("Text list as a ascii list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList("one", "two", "three")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlAscii))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List("one", "two", "three"))
  }

  test("Text list as a bigint list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(1l, 2l, 3l)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlBigint))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(1l, 2l, 3l))
  }

  test("Text list as a blob list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(ByteBuffer.wrap(Array[Byte](1,2,3,4,5)))
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlBlob))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(1) should equal(List("0x0102030405"))
  }

  test("Text list as a boolean list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(true, false)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlBoolean))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(true, false
    ))
  }

  test("Text list as a decimal list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(new java.math.BigDecimal("0.1"), new java.math.BigDecimal("0.2"))
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlDecimal))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(BigDecimal("0.1"), BigDecimal("0.2")))
  }

  test("Text list as a double list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(0.1, 0.2)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlDouble))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List("0.1", "0.2"))
  }


  test("Text list as a float list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(0.1f, 0.2f)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlFloat))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List("0.1", "0.2"))
  }

  test("Text list as a inet list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val localhost: InetAddress = InetAddress.getByName("127.0.0.1")
    val listVariable = util.Arrays.asList(localhost)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlInet))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(localhost.getHostAddress))
  }

  test("Text list as a int list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = util.Arrays.asList(1,2,3,4)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlInt))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(1,2,3,4))
  }

  test("Text list as a timestamp list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val now = new Date()
    val listVariable = util.Arrays.asList(now)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlTimestamp))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(now.getTime))
  }

  test("Text list as a timeuuid list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val timeuuid = UUID.fromString("2b329cc0-73f0-11e4-ac06-4b05b98cc84c")
    val listVariable = util.Arrays.asList(timeuuid, timeuuid)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlTimeUUID))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(timeuuid.toString, timeuuid.toString))
  }

  test("Text list as a uuid list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val uuid = UUID.randomUUID()
    val listVariable = util.Arrays.asList(uuid, uuid)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlUUID))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(uuid.toString, uuid.toString))
  }

  test("Text list as a varint list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val bigInt = new BigInteger("1234")
    val listVariable = util.Arrays.asList(bigInt, bigInt)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlList(CqlVarint))

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(0) should equal(BigDecimal(id.toString))
    executions(0).variables(1) should equal(List(BigDecimal("1234"), BigDecimal("1234")))
  }
}
