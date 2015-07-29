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

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit._
import akka.util.ByteString
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.server.actors.MessageHelper.createQueryMessage
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.response._
import org.scassandra.server.cqlmessages.types.{CqlInt, CqlVarchar, CqlText}
import org.scassandra.server.priming.query.{Prime, PrimeMatch, PrimeQueryStore}
import org.scassandra.server.priming.{Query, _}

class QueryHandlerTest extends FunSuite with Matchers with BeforeAndAfter with TestKitBase with MockitoSugar {
  implicit lazy val system = ActorSystem()

  var underTest: ActorRef = null
  var testProbeForTcpConnection: TestProbe = null
  val mockPrimedResults = mock[PrimeQueryStore]
  val someCqlStatement = PrimeMatch("some cql statement", ONE)
  val cqlMessageFactory = VersionTwoMessageFactory
  val protocolVersion: Byte = ProtocolVersion.ServerProtocolVersionTwo
  val activityLog = new ActivityLog
  implicit val impProtocolVersion = VersionTwo

  before {
    activityLog.clearQueries()
    testProbeForTcpConnection = TestProbe()
    underTest = TestActorRef(new QueryHandler(testProbeForTcpConnection.ref, mockPrimedResults, cqlMessageFactory, activityLog))
    reset(mockPrimedResults)
  }

  test("Should return set keyspace message for use statement") {
    val useStatement: String = "use keyspace"
    val stream: Byte = 0x02

    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(useStatement).toArray.drop(8))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(SetKeyspace("keyspace", stream))
  }

  test("Should return empty result when PrimedResults returns None") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(None)

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Rows("","",stream, Map()))
  }

  test("Should return empty rows result when PrimedResults returns empty list") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(PrimeMatch(someCqlStatement.query, ONE))).thenReturn(Some(Prime(List())))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Rows("", "", stream, Map()))
  }

  test("Should return rows result when PrimedResults returns a list of rows") {
    val stream: Byte = 0x05
    val rows: List[Row] = List(
      Row(Map(
        "name" -> "Mickey",
        "age" -> "99"
      ))
    )
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query).toArray.drop(8))
    when(mockPrimedResults.get(PrimeMatch(someCqlStatement.query, ONE))).thenReturn(Some(Prime(List[Map[String, Any]](
      Map(
        "name" -> "Mickey",
        "age" -> "99"
      )
    ),
      SuccessResult,
      Map(
        "name" -> CqlVarchar,
        "age" -> CqlInt
      ))))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Rows("", "", stream, Map("name" -> CqlVarchar, "age" -> CqlInt), rows))
  }

  test("Should return ReadRequestTimeout if result is ReadTimeout") {
    val stream: Byte = 0x05
    val consistency = LOCAL_QUORUM
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query, stream, consistency).toArray.drop(8))
    when(mockPrimedResults.get(PrimeMatch("some cql statement", consistency))).thenReturn(Some(Prime(List(), ReadRequestTimeoutResult())))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(ReadRequestTimeout(stream, consistency, ReadRequestTimeoutResult()))
  }

  test("Should return WriteRequestTimeout if result is WriteTimeout") {
    val stream: Byte = 0x05
    val consistency = LOCAL_ONE
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query, consistency = consistency).toArray.drop(8))
    val result: WriteRequestTimeoutResult = WriteRequestTimeoutResult()
    when(mockPrimedResults.get(PrimeMatch("some cql statement", consistency))).thenReturn(Some(Prime(List(), result)))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(WriteRequestTimeout(stream, consistency, result))
  }

  test("Should return Unavailable Exception if result is UnavailableException") {
    val query = "some cql statement"
    val stream: Byte = 0x05
    val consistency: Consistency = LOCAL_ONE
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(query, consistency = consistency).toArray.drop(8))
    val result: UnavailableResult = UnavailableResult()
    when(mockPrimedResults.get(PrimeMatch(query, consistency))).thenReturn(Some(Prime(List(), result)))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(UnavailableException(stream, consistency, result))
  }

  test("Test multiple rows") {
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query).toArray.drop(8))
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
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(rows, SuccessResult, colTypes)))

    underTest ! QueryHandler.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Rows("", "", stream, Map("name" -> CqlVarchar, "age" -> CqlVarchar),
      rows.map(row => Row(row))))
  }

  test("Should store query in the ActivityLog even if not primed") {
    //given
    activityLog.clearQueries()
    val stream: Byte = 1
    val query = "select * from people"
    val consistency = TWO
    val queryBody: ByteString = ByteString(createQueryMessage(query, consistency = consistency).toArray.drop(8))
    when(mockPrimedResults.get(PrimeMatch(query, consistency))).thenReturn(None)

    //when
    underTest ! QueryHandler.Query(queryBody, stream)

    //then
    val recordedQueries = activityLog.retrieveQueries()
    recordedQueries.size should equal(1)
    val recordedQuery: Query = recordedQueries.head
    recordedQuery should equal(Query(query, consistency))
  }

  test("Should return keyspace name when set in PrimedResults") {
    // given
    val stream: Byte = 0x05
    val someQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query).toArray.drop(8))
    val expectedKeyspace = "somekeyspace"

    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), SuccessResult, Map(), expectedKeyspace)))

    // when
    underTest ! QueryHandler.Query(someQuery, stream)

    // then
    testProbeForTcpConnection.expectMsg(Rows(expectedKeyspace, "", stream, Map()))
  }

  test("Should return table name when set in PrimedResults") {
    // given
    val stream: Byte = 0x05
    val someQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query).toArray.drop(8))
    val expectedTable = "sometable"

    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), SuccessResult, Map(), "", expectedTable)))

    // when
    underTest ! QueryHandler.Query(someQuery, stream)

    // then
    testProbeForTcpConnection.expectMsg(Rows("", expectedTable, stream, Map()))
  }

  test("Query with parameters - single text parameter") {
    // given
    val stream: Byte = 0x05
    val queryFlag = QueryFlags.Values
    val someQuery: ByteString = ByteString(createQueryMessage(someCqlStatement.query, flags = queryFlag).toArray.drop(8))
    val queryParams = ByteString(CqlProtocolHelper.serializeShort(1) ++ // number of parameters
                      CqlText.writeValueWithLength("Hello"))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(List(), SuccessResult, Map(), "", "sometable", variableTypes = List(CqlText))))

    // when
    underTest ! QueryHandler.Query(someQuery ++ queryParams, stream)

    // then
    testProbeForTcpConnection.expectMsg(Rows("", "sometable", stream, Map()))
    val recordedQueries = activityLog.retrieveQueries()
    recordedQueries.size should equal(1)
    val recordedQuery: Query = recordedQueries.head
    recordedQuery should equal(Query(someCqlStatement.query, someCqlStatement.consistency, List(Some("Hello")), List(CqlText)))
  }
}