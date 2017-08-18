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

import akka.actor.ActorRef
import akka.testkit._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers, WordSpec}
import org.scassandra.codec._
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages.{QueryParameters, Row}
import org.scassandra.server.actors.ActivityLogActor.RecordQuery
import org.scassandra.server.priming.query.{PrimeQueryStore, Reply}

import scala.concurrent.duration._
import scala.language.postfixOps

class QueryHandlerTest extends WordSpec with ImplicitSender with ProtocolActorTest with Matchers with BeforeAndAfter
  with TestKitWithShutdown with MockitoSugar {
  implicit val protocolVersion = ProtocolVersion.latest

  var underTest: ActorRef = null
  var tcpConnectionTestProbe: TestProbe = null
  val mockPrimedResults = mock[PrimeQueryStore]
  val someCqlStatement = Query("some cql statement", QueryParameters(consistency = Consistency.ONE))
  val activityLogProbe = TestProbe()
  val activityLog = activityLogProbe.ref

  before {
    underTest = TestActorRef(new QueryHandler(mockPrimedResults, activityLog))
    reset(mockPrimedResults)

    receiveWhile(10 milliseconds) {
      case _ =>
    }
    activityLogProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
  }

  "query handler" must {

    "return empty result when PrimeQueryStore returns None" in {
      when(mockPrimedResults.apply(someCqlStatement)).thenReturn(None)

      underTest ! protocolMessage(someCqlStatement)

      expectMsgPF() {
        case ProtocolResponse(_, `NoRows`) => true
      }
    }

    "Should return Prime message when PrimeQueryStore returns Prime" in {
      val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
      when(mockPrimedResults.apply(someCqlStatement)).thenReturn(Some(Reply(rows)))

      underTest ! protocolMessage(someCqlStatement)

      expectMsgPF() {
        case ProtocolResponse(_, `rows`) => true
      }
    }

    "store query in the ActivityLog even if not primed" in {
      //given
      val queryText = "select * from people"
      val consistency = Consistency.TWO
      val query = Query(queryText, QueryParameters(consistency = consistency))
      when(mockPrimedResults.apply(query)).thenReturn(None)

      //when
      underTest ! protocolMessage(query)

      //then
      activityLogProbe.expectMsg(RecordQuery(Activity.Query(query.query, consistency, None, List(), List(), None)))
    }

    "record query parameter values from request in QueryLog if Prime contains variable types" in {
      // given
      val consistency = Consistency.THREE

      val variableTypes: List[DataType] = List(Varchar, CqlInt)
      val values: List[Any] = List("Hello", 42)
      val rawValues: List[QueryValue] = values.zip(variableTypes).map {
        case (v, dataType) =>
          QueryValue(None, Bytes(dataType.codec.encode(v).require.toByteVector))
      }
      val query = Query("select * from sometable where k = ?", QueryParameters(consistency = consistency, values = Some(rawValues)))

      when(mockPrimedResults.apply(query)).thenReturn(Some(Reply(NoRows, variableTypes = Some(variableTypes))))

      // when
      underTest ! protocolMessage(query)

      // then
      expectMsgPF() {
        case ProtocolResponse(_, `NoRows`) => true
      }

      // check activityLog
      activityLogProbe.expectMsg(RecordQuery(Activity.Query(query.query, consistency, None, values, variableTypes)))
    }
  }
}