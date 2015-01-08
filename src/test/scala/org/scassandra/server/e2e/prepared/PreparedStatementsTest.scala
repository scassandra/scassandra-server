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

import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.{PrimingHelper, AbstractIntegrationTest}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.priming.prepared.{PrimePreparedSingle, ThenPreparedSingle, WhenPreparedSingle}
import scala.Some
import java.nio.ByteBuffer
import akka.util.ByteString
import java.util.{UUID, Date}
import com.datastax.driver.core.utils.UUIDs
import java.net.InetAddress
import java.util
import com.datastax.driver.core.{ConsistencyLevel, Row}
import org.scassandra.server.priming._
import com.datastax.driver.core.exceptions.{UnavailableException, WriteTimeoutException, ReadTimeoutException}
import org.scalatest.BeforeAndAfter
import dispatch._, Defaults._
import spray.json._
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.priming.ConflictingPrimes
import org.scassandra.server.priming.prepared.ThenPreparedSingle
import org.scassandra.server.priming.prepared.WhenPreparedSingle
import org.scassandra.server.priming.prepared.PrimePreparedSingle

class PreparedStatementsTest extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  import org.scassandra.server.priming.PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Prepared statement without priming - no params") {
    //given
    val preparedStatement = session.prepare("select * from people")
    val boundStatement = preparedStatement.bind()

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement without priming - single params") {
    //given
    val preparedStatement = session.prepare("select * from people where name = ?")
    val boundStatement = preparedStatement.bind("name")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement for schema change") {
    //given
    val preparedStatement = session.prepare("CREATE KEYSPACE ? WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ?, 'dc2': ?};")
    val boundStatement = preparedStatement.bind("keyspaceName","3","1")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - empty rows") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List()))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - read_request_timeout") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(None, result = Some(ReadTimeout))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    intercept[ReadTimeoutException] {
      session.execute(boundStatement)
    }
  }

  test("Prepared statement with priming - write_request_timeout") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(None, result = Some(WriteTimeout))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    intercept[WriteTimeoutException] {
      session.execute(boundStatement)
    }
  }

  test("Prepared statement with priming - unavailable") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(None, result = Some(Unavailable))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    intercept[UnavailableException] {
      session.execute(boundStatement)
    }
  }

  test("Prepared statement with priming - single row") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  test("Prime for a specific consistency. Expecting no results for a different consistency.") {
    //given
    val preparedStatementText: String = "select * from people where name = ?"
    val consistencyToPrime = List(TWO)
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText), consistency = Some(consistencyToPrime)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    preparedStatement.setConsistencyLevel(ConsistencyLevel.ONE)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("prime for a specific consistency. Get results back.") {
    //given
    val preparedStatementText: String = "select * from people where name = ?"
    val consistencyToPrime = List(QUORUM)
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText), None, Some(consistencyToPrime)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    preparedStatement.setConsistencyLevel(ConsistencyLevel.QUORUM)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }
  
  test("Conflicting primes") {
    //given
    val preparedStatementText = "select * from people where name = ?"
    val consistencyOneAndTwo = List(ONE, TWO)
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText), consistency = Some(consistencyOneAndTwo)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )

    //when
    val consistencyTwoAndThree = List(TWO, THREE)
    val prime = PrimePreparedSingle(WhenPreparedSingle(Some(preparedStatementText), consistency = Some(consistencyTwoAndThree)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))).toJson
    val svc = url("http://localhost:8043/prime-prepared-single") <<
      prime.toString <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc > as.String)

    whenReady(response) {
      result =>
        val conflictingPrime = JsonParser(result).convertTo[ConflictingPrimes]
        conflictingPrime.existingPrimes.size should equal(1)
    }
  }

  test("Prepared statement - priming numeric parameters") {
    //given
    val preparedStatementText = "insert into people(bigint, counter, decimal, double, float, int, varint) = (?, ?,?,?,?,?,?)"
    val resultColumnTypes = Map[String, ColumnType[_]]("bigint" -> CqlBigint,
      "counter" -> CqlCounter,
      "decimal" -> CqlDecimal,
      "double" -> CqlDouble,
      "float" -> CqlFloat,
      "int" -> CqlInt,
      "varint" -> CqlVarint)
    val bigInt : java.lang.Long = 1234
    val counter : java.lang.Long = 2345
    val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
    val double : java.lang.Double = 1.5
    val float : java.lang.Float = 2.5f
    val int : java.lang.Integer = 3456
    val varint : java.math.BigInteger = new java.math.BigInteger("123")

    val rows: List[Map[String, Any]] = List(
      Map(
        "bigint" -> bigInt.toString,
        "counter" -> counter.toString,
        "decimal" -> decimal.toString,
        "double" -> double.toString,
        "float" -> float.toString,
        "int" -> int.toString,
        "varint" -> varint.toString
      )
    )
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(Some(rows),
        Some(List[ColumnType[_]](CqlBigint, CqlCounter, CqlDecimal, CqlDouble, CqlFloat, CqlInt, CqlVarint)),
        Some(resultColumnTypes))
    )

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(bigInt, counter, decimal, double, float, int, varint)
    val result = session.execute(boundStatement)

    //then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)
    resultRow.getLong("bigint") should equal(bigInt)
    resultRow.getLong("counter") should equal(counter)
    resultRow.getDecimal("decimal") should equal(decimal)
    resultRow.getDouble("double") should equal(double)
    resultRow.getFloat("float") should equal(float)
    resultRow.getInt("int") should equal(int)
    resultRow.getVarint("varint") should equal(varint)
  }

  test("Prepared statement - priming non-numeric parameters") {
    //given
    val preparedStatementText = "insert into people(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet) = (?,?,?,?,?,?,?,?)"
    val resultColumnTypes: Map[String, ColumnType[_]] = Map[String, ColumnType[_]](
      "ascii" -> CqlAscii,
      "blob" -> CqlBlob,
      "boolean" -> CqlBoolean,
      "timestamp" -> CqlTimestamp,
      "uuid" -> CqlUUID,
      "varchar" -> CqlVarchar,
      "timeuuid" -> CqlTimeUUID,
      "inet" -> CqlInet
    )

    val ascii : String = "ascii"
    val blob : ByteBuffer = ByteString().toByteBuffer
    val boolean : java.lang.Boolean = true
    val timestamp : java.util.Date = new Date();
    val uuid : UUID = UUID.randomUUID()
    val varchar : String = "varchar"
    val timeuuid : UUID = UUIDs.timeBased()
    val inet : InetAddress = InetAddress.getByName("127.0.0.1")

    val primedRow = Map(
      "ascii" -> ascii.toString,
      "blob" -> "0x",
      "boolean" -> boolean,
      "timestamp" -> timestamp.getTime,
      "uuid" -> uuid.toString,
      "varchar" -> varchar,
      "timeuuid" -> timeuuid.toString,
      "inet" -> "127.0.0.1"
    )

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(primedRow)),
        variable_types = Some(List[ColumnType[_]](CqlAscii, CqlBlob, CqlBoolean, CqlTimestamp, CqlUUID, CqlVarchar, CqlTimeUUID, CqlInet)),
        column_types = Some(resultColumnTypes))
    )

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet)
    val result = session.execute(boundStatement)

    //then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)

    resultRow.getString("ascii") should equal(ascii)
    resultRow.getBytes("blob") should equal(blob)
    resultRow.getBool("boolean") should equal(boolean)
    resultRow.getDate("timestamp") should equal(timestamp)
    resultRow.getUUID("uuid") should equal(uuid)
    resultRow.getString("varchar") should equal(varchar)
    resultRow.getUUID("timeuuid") should equal(timeuuid)
    resultRow.getInet("inet") should equal(inet)
  }
}
