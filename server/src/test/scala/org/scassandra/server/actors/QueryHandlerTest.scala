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
package org.scassandra.server.actors

import akka.actor.ActorRef
import akka.testkit._
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scassandra.codec._
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages.{QueryParameters, Row}
import org.scassandra.server.actors.ActivityLogActor.RecordQuery
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{MatchPrime, MatchResult, Reply}

import scala.concurrent.duration._
import scala.language.postfixOps

class QueryHandlerTest extends WordSpec with ImplicitSender with ProtocolActorTest with Matchers with BeforeAndAfter
  with TestKitWithShutdown {
  implicit val protocolVersion = ProtocolVersion.latest

  var underTest: ActorRef = null
  var tcpConnectionTestProbe: TestProbe = null
  val someCqlStatement = Query("some cql statement", QueryParameters(consistency = Consistency.ONE))
  val activityLogProbe = TestProbe()
  val activityLog = activityLogProbe.ref
  val primeQueryStoreProbe = TestProbe()
  val primeQueryStore = primeQueryStoreProbe.ref

  before {
    underTest = TestActorRef(new QueryHandler(primeQueryStore, activityLog))

    receiveWhile(10 milliseconds) {
      case _ =>
    }
    activityLogProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
    primeQueryStoreProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
  }

  "query handler" must {

    "return empty result when PrimeQueryStore returns None" in {
      respondWith(primeQueryStoreProbe, MatchResult(None))

      underTest ! protocolMessage(someCqlStatement)

      primeQueryStoreProbe.expectMsg(MatchPrime(someCqlStatement))
      expectMsgPF() {
        case ProtocolResponse(_, `NoRows`) => true
      }
    }

    "return Prime message when PrimeQueryStore returns Prime" in {
      val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
      respondWith(primeQueryStoreProbe, MatchResult(Some(Reply(rows))))

      underTest ! protocolMessage(someCqlStatement)

      primeQueryStoreProbe.expectMsg(MatchPrime(someCqlStatement))
      expectMsgPF() {
        case ProtocolResponse(_, r) if r == rows =>
      }
    }

    "store query in the ActivityLog even if not primed" in {
      //given
      val queryText = "select * from people"
      val consistency = Consistency.TWO
      val query = Query(queryText, QueryParameters(consistency = consistency))
      respondWith(primeQueryStoreProbe, MatchResult(None))

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
      val query = Query("select * from someTable where k = ?", QueryParameters(consistency = consistency, values = Some(rawValues)))
      respondWith(primeQueryStoreProbe, MatchResult(Some(Reply(NoRows, variableTypes = Some(variableTypes)))))

      underTest ! protocolMessage(query)

      expectMsgPF() {
        case ProtocolResponse(_, NoRows) =>
      }
      activityLogProbe.expectMsg(RecordQuery(Activity.Query(query.query, consistency, None, values, variableTypes)))
    }
  }

  private def respondWith(probe: TestProbe, m: Any): Unit = {
    probe.setAutoPilot((sender: ActorRef, msg: Any) => {
      msg match {
        case _ =>
          sender ! m
          TestActor.NoAutoPilot
      }
    })
  }
}