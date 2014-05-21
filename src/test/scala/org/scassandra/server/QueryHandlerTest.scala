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

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp.Write
import akka.testkit._
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import org.scassandra.priming._
import org.mockito.Mockito._
import org.scassandra.cqlmessages._
import org.scassandra.cqlmessages.response.ReadRequestTimeout
import org.scassandra.cqlmessages.response.VoidResult
import scala.Some
import org.scassandra.cqlmessages.response.WriteRequestTimeout
import org.scassandra.cqlmessages.response.Row
import org.scassandra.cqlmessages.response.SetKeyspace
import org.scassandra.cqlmessages.response.UnavailableException
import org.scassandra.cqlmessages.response.Rows
import org.scassandra.priming.Query
import org.scassandra.priming.query.{PrimeQueryStore, Prime, PrimeMatch}

class QueryHandlerTest extends FunSuite with ShouldMatchers with BeforeAndAfter with TestKitBase with MockitoSugar {
  implicit lazy val system = ActorSystem()

  var underTest: ActorRef = null
  var testProbeForTcpConnection: TestProbe = null
  val mockPrimedResults = mock[PrimeQueryStore]
  val someCqlStatement = PrimeMatch("some cql statement", ONE)
  val cqlMessageFactory = VersionTwoMessageFactory
  val protocolVersion: Byte = ProtocolVersion.ServerProtocolVersionTwo
  implicit val impProtocolVersion = VersionTwo

  before {
    testProbeForTcpConnection = TestProbe()
    underTest = TestActorRef(new QueryHandler(testProbeForTcpConnection.ref, mockPrimedResults, cqlMessageFactory))
    reset(mockPrimedResults)
  }

  test("Should return set keyspace message for use statement") {
    val useStatement: String = "use keyspace"
    val stream: Byte = 0x02

    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(useStatement).toArray.drop(8))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(SetKeyspace("keyspace", stream).serialize()))
  }

  test("Should return empty result when PrimedResults returns None") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(None)

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("","",stream, Map()).serialize()))
  }

  test("Should return empty rows result when PrimedResults returns empty list") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(PrimeMatch(someCqlStatement.query, ONE))).thenReturn(Some(Prime(List())))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("", "", stream, Map()).serialize()))
  }

  test("Should return rows result when PrimedResults returns a list of rows") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(PrimeMatch(someCqlStatement.query, ONE))).thenReturn(Some(Prime(List[Map[String, Any]](
      Map(
        "name" -> "Mickey",
        "age" -> 99
      )
    ),
      Success,
      Map(
        "name" -> CqlVarchar,
        "age" -> CqlInt
      ))))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("", "", stream, Map("name" -> CqlVarchar, "age" -> CqlInt), List(
      Row(Map(
        "name" -> "Mickey",
        "age" -> 99
      ))
    )).serialize()))
  }

  test("Should return ReadRequestTimeout if result is ReadTimeout") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), ReadTimeout)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(ReadRequestTimeout(stream).serialize()))
  }

  test("Should return WriteRequestTimeout if result is WriteTimeout") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), WriteTimeout)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(WriteRequestTimeout(stream).serialize()))
  }

  test("Should return Unavailable Exception if result is UnavailableException") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), Unavailable)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(UnavailableException(stream).serialize()))
  }

  test("Test multiple rows") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    val rows = List[Map[String, String]](
      Map(
        "name" -> "Mickey",
        "age" -> "99"
      ),
      Map(
        "name" -> "Jenifer",
        "age" -> "88"
      )
    )
    val colTypes = Map(
      "name" -> CqlVarchar,
      "age" -> CqlVarchar
    )
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(rows, Success, colTypes)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("", "", stream, Map("name" -> CqlVarchar, "age" -> CqlVarchar),
      rows.map(row => Row(row))).serialize()))
  }

  test("Should store query in the ActivityLog") {
    //given
    ActivityLog.clearQueries()
    val stream: Byte = 1
    val query = "select * from people"
    val consistency = TWO
    val queryBody: ByteString = ByteString(MessageHelper.createQueryMessage(query, consistency = consistency).toArray.drop(8))

    //when
    underTest ! QueryHandlerMessages.Query(queryBody, stream)

    //then
    val recordedQueries = ActivityLog.retrieveQueries()
    recordedQueries.size should equal(1)
    val recordedQuery: Query = recordedQueries(0)
    recordedQuery should equal(Query(query, consistency))
  }

  test("Should return keyspace name when set in PrimedResults") {
    // given
    val stream: Byte = 0x05
    val someQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    val expectedKeyspace = "somekeyspace"

    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), Success, Map(), expectedKeyspace)))

    // when
    underTest ! QueryHandlerMessages.Query(someQuery, stream)

    // then
    testProbeForTcpConnection.expectMsg(Write(Rows(expectedKeyspace, "", stream, Map()).serialize()))
  }

  test("Should return table name when set in PrimedResults") {
    // given
    val stream: Byte = 0x05
    val someQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement.query).toArray.drop(8))
    val expectedTable = "sometable"

    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), Success, Map(), "", expectedTable)))

    // when
    underTest ! QueryHandlerMessages.Query(someQuery, stream)

    // then
    testProbeForTcpConnection.expectMsg(Write(Rows("", expectedTable, stream, Map()).serialize()))
  }

}