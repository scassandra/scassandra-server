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
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages.{ColumnSpecWithoutTable, NoRowMetadata, PreparedMetadata}
import org.scassandra.codec.{Prepare, Prepared}
import org.scassandra.server.actors.Activity.PreparedStatementPreparation
import org.scassandra.server.actors.ActivityLogActor.RecordPrepare
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{LookupByPrepare, PrimeMatch}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.Reply
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.language.postfixOps

class PrepareHandlerTest extends WordSpec with ProtocolActorTest with ImplicitSender with Matchers with TestKitWithShutdown with ScalaFutures
  with BeforeAndAfter {

  var underTest: ActorRef = _
  private val activityLogProbe = TestProbe()
  private val activityLog: ActorRef = activityLogProbe.ref
  private val primePreparedStoreProbe = TestProbe()
  private val primePreparedStore = primePreparedStoreProbe.ref

  val id = ByteVector(1)

  implicit val timeout: Timeout = 1 seconds

  before {
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))
    receiveWhile(10 milliseconds) {
      case _ =>
    }
    activityLogProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
  }

  "a prepare handler" must {
    "return prepared message on prepare - no params" in {
      val prepare = Prepare("select * from something")
      respondWith(primePreparedStoreProbe, PrimeMatch(None))

      underTest ! protocolMessage(prepare)

      primePreparedStoreProbe.expectMsg(LookupByPrepare(prepare, 1))
      expectMsgPF() {
        case ProtocolResponse(_, Prepared(_, PreparedMetadata(Nil, Some("keyspace"), Some("table"), Nil), NoRowMetadata)) => true
      }
    }

    "return prepared message on prepare - with params" in {
      val prepare = Prepare("select * from something where name = ?")
      respondWith(primePreparedStoreProbe, PrimeMatch(None))

      underTest ! protocolMessage(prepare)

      primePreparedStoreProbe.expectMsg(LookupByPrepare(prepare, 1))
      expectMsgPF() {
        case ProtocolResponse(_, Prepared(_, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
        List(ColumnSpecWithoutTable("0", Varchar))), NoRowMetadata)) => true
      }
    }

    "use types from Prime" in {
      val prepare = Prepare("select * from something where name = ?")
      val prepared = Prepared(id, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
        List(ColumnSpecWithoutTable("0", CqlInt))))
      val prime = Some(Reply(prepared))
      respondWith(primePreparedStoreProbe, PrimeMatch(prime))

      underTest ! protocolMessage(Prepare("select * from something where name = ?"))

      primePreparedStoreProbe.expectMsg(LookupByPrepare(prepare, 1))
    }

    "use incrementing ids" in {
      val prepare1 = Prepare("select * from cats")
      val prepare2 = Prepare("select * from dogs")

      underTest ! protocolMessage(prepare1)
      underTest ! protocolMessage(prepare2)

      primePreparedStoreProbe.expectMsg(LookupByPrepare(prepare1, 1))
      primePreparedStoreProbe.expectMsg(LookupByPrepare(prepare2, 2))
    }
  }

  "record preparation in activity log" in {
    underTest ! protocolMessage(Prepare("select 1"))

    activityLogProbe.expectMsg(RecordPrepare(PreparedStatementPreparation("select 1")))
  }

  "answer queries for prepared statement - not exist" in {
    val response = (underTest ? PreparedStatementQuery(List(1))).mapTo[PreparedStatementResponse]

    response.futureValue should equal(PreparedStatementResponse(Map()))
  }

  "answer queries for prepared statement - exists" in {
    val query = "select * from something where name = ?"
    val prepared = Prepared(id, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
      List(ColumnSpecWithoutTable("0", CqlInt))))
    respondWith(primePreparedStoreProbe, PrimeMatch(Some(Reply(prepared))))

    underTest ! protocolMessage(Prepare(query))

    val response = (underTest ? PreparedStatementQuery(List(id.toInt()))).mapTo[PreparedStatementResponse]

    response.futureValue should equal(PreparedStatementResponse(Map(id.toInt() -> (query, prepared))))
  }
}
