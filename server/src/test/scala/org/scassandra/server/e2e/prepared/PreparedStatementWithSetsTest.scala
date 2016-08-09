/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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
import java.util.{Date, UUID}

import com.google.common.collect.Sets
import dispatch.Defaults._
import dispatch._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}

class PreparedStatementWithSetsTest  extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()

    val svcDeleteActivity = url("http://localhost:8043/prepared-statement-execution").DELETE
    val responseDeleteActivity = Http(svcDeleteActivity OK as.String)
    responseDeleteActivity()
  }

  test("Text set as a varchar list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet("one", "two", "three")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlVarchar))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("one", "two", "three"))
  }

  test("Text set as a text list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet("one", "two", "three")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlText))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("one", "two", "three"))
  }

  test("Text set as a ascii list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet("one", "two", "three")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlAscii))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("one", "two", "three"))
  }

  test("Text set as a bigint list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(1l, 2l, 3l)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlBigint))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(1l, 2l, 3l))
  }

  test("Text set as a blob list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(ByteBuffer.wrap(Array[Byte](1,2,3,4,5)))
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlBlob))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("0x0102030405"))
  }

  test("Text set as a boolean list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(true, false)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlBoolean))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(true, false
    ))
  }

  test("Text set as a decimal list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(new java.math.BigDecimal("0.1"), new java.math.BigDecimal("0.2"))
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlDecimal))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("0.1", "0.2"))
  }

  test("Text set as a double list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(0.1, 0.2)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlDouble))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("0.1", "0.2"))
  }


  test("Text set as a float list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(0.1f, 0.2f)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlFloat))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set("0.1", "0.2"))
  }

  test("Text set as a inet list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val localhost: InetAddress = InetAddress.getByName("127.0.0.1")
    val listVariable = Sets.newHashSet(localhost)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlInet))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(localhost.getHostAddress))
  }

  test("Text set as a int list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val listVariable = Sets.newHashSet(1,2,3,4)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlInt))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(1,2,3,4))
  }

  test("Text set as a timestamp list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val now = new Date()
    val listVariable = Sets.newHashSet(now)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlTimestamp))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(now.getTime))
  }

  test("Text set as a timeuuid list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val timeuuid = UUID.fromString("2b329cc0-73f0-11e4-ac06-4b05b98cc84c")
    val listVariable = Sets.newHashSet(timeuuid, timeuuid)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlTimeUUID))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(timeuuid.toString, timeuuid.toString))
  }

  test("Text set as a uuid list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val uuid = UUID.randomUUID()
    val listVariable = Sets.newHashSet(uuid, uuid)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlUUID))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(rows = None, variable_types = Some(variableTypes))
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(id, listVariable)
    session.execute(boundStatement)

    // then
    val executions = PrimingHelper.getRecordedPreparedStatements()
    executions.size should equal(1)
    executions.head.variables(0) should equal(BigDecimal(id.toString))
    executions.head.variables(1).asInstanceOf[List[_]].toSet should equal(Set(uuid.toString, uuid.toString))
  }

  test("Text set as a varint list variable") {
    val preparedStatementText = "insert into some_table(id, list) values(?, ?)"
    val id: Integer = Int.box(1)
    val bigInt = new BigInteger("1234")
    val listVariable = Sets.newHashSet(bigInt, bigInt)
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlSet(CqlVarint))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
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
    executions(0).variables(1).asInstanceOf[List[_]].toSet should equal(Set(BigDecimal("1234"), BigDecimal("1234")))
  }
}
