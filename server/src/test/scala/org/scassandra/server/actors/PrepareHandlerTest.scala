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
package org.scassandra.server.actors

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestKitBase, TestProbe}
import akka.util.{Timeout, ByteString}
import akka.pattern.ask

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementResponse, PreparedStatementQuery}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.request.{ExecuteRequestV2, PrepareRequest, _}
import org.scassandra.server.cqlmessages.response.{PreparedResultV2, ReadRequestTimeout, Rows, UnavailableException, VoidResult, WriteRequestTimeout}
import org.scassandra.server.cqlmessages.types.{ColumnType, CqlBigint, CqlInt, CqlVarchar}
import org.scassandra.server.priming.prepared.{PreparedPrime, PrimePreparedStore}
import org.scassandra.server.priming.query.{Prime, PrimeMatch}
import org.scassandra.server.priming.{PreparedStatementExecution, _}


class PrepareHandlerTest extends FunSuite with Matchers with TestKitBase with BeforeAndAfter with MockitoSugar {
  implicit lazy val system = ActorSystem()

  var underTest: ActorRef = null
  var testProbeForTcpConnection: TestProbe = null
  val versionTwoMessageFactory = VersionTwoMessageFactory
  val versionOneMessageFactory = VersionOneMessageFactory
  val protocolVersion: Byte = ProtocolVersion.ServerProtocolVersionTwo
  implicit val impProtocolVersion = VersionTwo
  val activityLog: ActivityLog = new ActivityLog
  val primePreparedStore = mock[PrimePreparedStore]
  val stream: Byte = 0x3

  implicit val atMost: Duration = 1 seconds
  implicit val timeout: Timeout = 1 seconds

  before {
    reset(primePreparedStore)
    when(primePreparedStore.findPrime(any(classOf[PrimeMatch]))).thenReturn(None)
    testProbeForTcpConnection = TestProbe()
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))
  }

  test("Should return prepared message on prepare - no params") {
    val stream: Byte = 0x02
    val query = "select * from something"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandler.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List()))
  }

  test("Should return empty result message for execute - no params") {
    val stream: Byte = 0x02
    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, 1).serialize().drop(8)

    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(VoidResult(stream))
  }

  test("Should return  prepared message on prepare - single param") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandler.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List(CqlVarchar)))
  }

  test("Priming variable types - Should use types from PreparedPrime") {
    val query = "select * from something where name = ?"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)
    val primedVariableTypes = List(CqlInt)
    val preparedPrime: PreparedPrime = PreparedPrime(primedVariableTypes)
    when(primePreparedStore.findPrime(any(classOf[PrimeMatch]))).thenReturn(Some(preparedPrime))

    underTest ! PrepareHandler.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    verify(primePreparedStore).findPrime(PrimeMatch(query))
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", primedVariableTypes))
  }

  //todo may as well make these UUIDs
  test("Should use incrementing IDs") {
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))
    val stream: Byte = 0x02
    val queryOne = "select * from something where name = ?"
    val prepareBodyOne: ByteString = PrepareRequest(protocolVersion, stream, queryOne).serialize().drop(8)
    underTest ! PrepareHandler.Prepare(prepareBodyOne, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List(CqlVarchar)))

    emptyTestProbe

    val queryTwo = "select * from something where name = ? and age = ?"
    val prepareBodyTwo: ByteString = PrepareRequest(protocolVersion, stream, queryTwo).serialize().drop(8)
    underTest ! PrepareHandler.Prepare(prepareBodyTwo, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 2.toShort, "keyspace", "table", List(CqlVarchar, CqlVarchar)))

  }

  test("Should look up prepared prime in store with consistency & query - version 2") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val consistency = THREE

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    verify(primePreparedStore).findPrime(PrimeMatch(query, consistency))
  }

  test("Should look up prepared prime in store with consistency & query - version 1") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val consistency = THREE

    val executeBody: ByteString = ExecuteRequestV1(protocolVersion, stream, preparedStatementId, consistency).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionOneMessageFactory, testProbeForTcpConnection.ref)

    verify(primePreparedStore).findPrime(PrimeMatch(query, consistency))
  }

  test("Should create rows message if prime matches") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val primeMatch = Some(PreparedPrime())
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(Rows("" ,"" ,stream, Map(), List()))
  }

  test("Execute with read time out") {
    val query = "select * from something where name = ?"
    val consistency = QUORUM
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val primeMatch = Some(PreparedPrime(prime = Prime(result = ReadRequestTimeoutResult())))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency = consistency).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(ReadRequestTimeout(stream, consistency, ReadRequestTimeoutResult()))
  }

  test("Execute with write time out") {
    val query = "select * from something where name = ?"
    val consistency = QUORUM
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val result: WriteRequestTimeoutResult = WriteRequestTimeoutResult()
    val primeMatch = Some(PreparedPrime(prime = Prime(result = result)))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency = consistency).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(WriteRequestTimeout(stream, consistency, result))
  }

  test("Execute with unavailable") {
    val query = "select * from something where name = ?"
    val consistency = QUORUM
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val result: UnavailableResult = UnavailableResult()
    val primeMatch = Some(PreparedPrime(prime = Prime(result = result)))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency = consistency).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(UnavailableException(stream, consistency, result))
  }

  test("Should record preparation in activity log") {
    activityLog.clearPreparedStatementPreparations()
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandler.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    activityLog.retrievePreparedStatementPreparations().size should equal(1)
    activityLog.retrievePreparedStatementPreparations().head should equal(PreparedStatementPreparation(query))
  }

  test("Should record execution in activity log") {
    activityLog.clearPreparedStatementExecutions()
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val consistency = TWO
    val variableTypes = List(CqlBigint)
    val variables: List[Int] = List(10)
    val primeMatch = Some(PreparedPrime(variableTypes))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency, 1, variables, variableTypes = variableTypes).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions().head should equal(PreparedStatementExecution(query, consistency, variables.map(Some(_)), variableTypes))
  }

  test("Should record execution in activity log without variables when variables don't match prime") {
    activityLog.clearPreparedStatementExecutions()
    val stream: Byte = 0x02
    val query = "select * from something where name = ? and something = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query, List(CqlVarchar, CqlVarchar))
    val consistency = TWO
    val variableTypes = List(CqlBigint, CqlBigint)
    val variables: List[Int] = List(10, 20)
    val primeMatch = Some(PreparedPrime(List(CqlVarchar))) // prime has a single variable
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency, 1, variables, variableTypes = variableTypes).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions()(0) should equal(PreparedStatementExecution(query, consistency, List(), List()))
  }

  test("Should record execution in activity log event if not primed") {
    activityLog.clearPreparedStatementExecutions()
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val consistency = TWO
    val variableTypes = List(CqlBigint)
    val variables: List[Int] = List(10)
    val primeMatch = Some(PreparedPrime(variableTypes))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(None)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency, 1, variables, variableTypes = variableTypes).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions()(0) should equal(PreparedStatementExecution(query, consistency, List(), List()))
  }

  test("Should delay message if fixedDelay primed") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val prime = Prime(fixedDelay = Some(FiniteDuration(1500, TimeUnit.MILLISECONDS)))
    val primeMatch = Some(PreparedPrime(prime = prime))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandler.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    // i wish that expect msg took a min as well as a max :(
    testProbeForTcpConnection.expectMsg(Rows("" ,"" ,stream, Map(), List()))
  }

  test("Should answer queries for prepared statement - exists") {
    val query = "select * from something"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)
    underTest ! PrepareHandler.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    val response = (underTest ? PreparedStatementQuery(List(1))).mapTo[PreparedStatementResponse]

    Await.result(response, atMost) should equal(PreparedStatementResponse(Map(1 -> query)))
  }

  test("Should answer queries for prepared statement - not exist") {
    val response = (underTest ? PreparedStatementQuery(List(1))).mapTo[PreparedStatementResponse]

    Await.result(response, atMost) should equal(PreparedStatementResponse(Map()))
  }


  private def sendPrepareAndCaptureId(stream: Byte, query: String, variableTypes: List[ColumnType[_]] = List(CqlVarchar)) : Int = {
    val prepareBodyOne: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)
    underTest ! PrepareHandler.Prepare(prepareBodyOne, stream, versionTwoMessageFactory , testProbeForTcpConnection.ref)
    val preparedResponseWithId: PreparedResultV2 = testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", variableTypes))
    reset(primePreparedStore)
    when(primePreparedStore.findPrime(any(classOf[PrimeMatch]))).thenReturn(None)
    preparedResponseWithId.preparedStatementId
  }

  private def emptyTestProbe = {
    testProbeForTcpConnection.receiveWhile(idle = Duration(100, TimeUnit.MILLISECONDS))({
      case msg @ _ => println(s"Removing message from test probe $msg")
    })
  }
}
