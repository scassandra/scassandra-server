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
package org.scassandra.server

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestKitBase, TestProbe}
import akka.util.ByteString
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.request.{ExecuteRequestV2, PrepareRequest, _}
import org.scassandra.server.cqlmessages.response.{PreparedResultV2, ReadRequestTimeout, Rows, UnavailableException, VoidResult, WriteRequestTimeout}
import org.scassandra.server.cqlmessages.types.{CqlBigint, CqlInt, CqlVarchar}
import org.scassandra.server.priming.prepared.{PreparedPrime, PrimePreparedStore}
import org.scassandra.server.priming.query.{Prime, PrimeMatch}
import org.scassandra.server.priming.{PreparedStatementExecution, _}

import scala.concurrent.duration.{Duration, FiniteDuration}

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

  before {
    reset(primePreparedStore)
    when(primePreparedStore.findPrime(any(classOf[PrimeMatch]))).thenReturn(None)
    testProbeForTcpConnection = TestProbe()
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))
  }


  test("Should return result prepared message - no params") {
    val stream: Byte = 0x02
    val query = "select * from something"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandlerMessages.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List()))
  }

  test("Should return empty result message for execute - no params") {
    val stream: Byte = 0x02
    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, 1).serialize().drop(8)

    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(VoidResult(stream))
  }

  test("Should return result prepared message - single param") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandlerMessages.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List(CqlVarchar)))
  }

  test("Priming variable types - Should use types from PreparedPrime") {
    val query = "select * from something where name = ?"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)
    val primedVariableTypes = List(CqlInt)
    val preparedPrime: PreparedPrime = PreparedPrime(primedVariableTypes)
    when(primePreparedStore.findPrime(any(classOf[PrimeMatch]))).thenReturn(Some(preparedPrime))

    underTest ! PrepareHandlerMessages.Prepare(prepareBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    verify(primePreparedStore).findPrime(PrimeMatch(query))
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", primedVariableTypes))
  }
  
  test("Should use incrementing IDs") {
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))
    val stream: Byte = 0x02
    val queryOne = "select * from something where name = ?"
    val prepareBodyOne: ByteString = PrepareRequest(protocolVersion, stream, queryOne).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Prepare(prepareBodyOne, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List(CqlVarchar)))

    emptyTestProbe

    val queryTwo = "select * from something where name = ? and age = ?"
    val prepareBodyTwo: ByteString = PrepareRequest(protocolVersion, stream, queryTwo).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Prepare(prepareBodyTwo, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 2.toShort, "keyspace", "table", List(CqlVarchar, CqlVarchar)))

  }

  test("Should look up prepared prime in store with consistency & query - version 2") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val consistency = THREE

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId, consistency).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    verify(primePreparedStore).findPrime(PrimeMatch(query, consistency))
  }

  test("Should look up prepared prime in store with consistency & query - version 1") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val consistency = THREE

    val executeBody: ByteString = ExecuteRequestV1(protocolVersion, stream, preparedStatementId, consistency).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionOneMessageFactory, testProbeForTcpConnection.ref)

    verify(primePreparedStore).findPrime(PrimeMatch(query, consistency))
  }

  test("Should create rows message if prime matches") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val primeMatch = Some(PreparedPrime())
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(Rows("" ,"" ,stream, Map(), List()))
  }

  test("Execute with read time out") {
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val primeMatch = Some(PreparedPrime(prime = Prime(result = ReadTimeout)))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(ReadRequestTimeout(stream, ONE))
  }

  test("Execute with write time out") {
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val primeMatch = Some(PreparedPrime(prime = Prime(result = WriteTimeout)))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(WriteRequestTimeout(stream, ONE))
  }

  test("Execute with unavailable") {
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val primeMatch = Some(PreparedPrime(prime = Prime(result = Unavailable)))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(UnavailableException(stream))
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
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions()(0) should equal(PreparedStatementExecution(query, consistency, variables.map(Some(_)), variableTypes))
  }

  test("Should delay message if fixedDelay primed") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val preparedStatementId = sendPrepareAndCaptureId(stream, query)
    val prime = Prime(fixedDelay = Some(FiniteDuration(1500, TimeUnit.MILLISECONDS)))
    val primeMatch = Some(PreparedPrime(prime = prime))
    when(primePreparedStore.findPrime(any[PrimeMatch])).thenReturn(primeMatch)

    val executeBody: ByteString = ExecuteRequestV2(protocolVersion, stream, preparedStatementId).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Execute(executeBody, stream, versionTwoMessageFactory, testProbeForTcpConnection.ref)

    // i wish that expect msg took a min as well as a max :(
    testProbeForTcpConnection.expectMsg(Rows("" ,"" ,stream, Map(), List()))
  }


  private def sendPrepareAndCaptureId(stream: Byte, query: String) : Int = {
    val prepareBodyOne: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Prepare(prepareBodyOne, stream, versionTwoMessageFactory , testProbeForTcpConnection.ref)
    val preparedResponseWithId: PreparedResultV2 = testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", List(CqlVarchar)))
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
