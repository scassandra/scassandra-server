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
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import akka.util.Timeout
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages.{ColumnSpecWithoutTable, NoRowMetadata, PreparedMetadata, RowMetadata}
import org.scassandra.codec.{Prepare, Prepared}
import org.scassandra.server.actors.Activity.PreparedStatementPreparation
import org.scassandra.server.actors.ActivityLogActor.RecordPrepare
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.Reply
import org.scassandra.server.priming.prepared.PrimePreparedStore
import scodec.bits.ByteVector

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


class PrepareHandlerTest extends WordSpec with ProtocolActorTest with ImplicitSender with Matchers with TestKitWithShutdown
  with BeforeAndAfter with MockitoSugar {

  var underTest: ActorRef = null
  val activityLogProbe = TestProbe()
  val activityLog: ActorRef = activityLogProbe.ref
  val primePreparedStore = mock[PrimePreparedStore]

  def anyFunction() = any[Function2[PreparedMetadata, RowMetadata, Prepared]]

  val id = ByteVector(1)

  implicit val atMost: Duration = 1 seconds
  implicit val timeout: Timeout = 1 seconds

  before {
    reset(primePreparedStore)
    when(primePreparedStore.apply(any(classOf[Prepare]), anyFunction())).thenReturn(None)
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))

    receiveWhile(10 milliseconds) {
      case _ =>
    }
    activityLogProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
  }

  "a prepare hadler" must {
    "return prepared message on prepare - no params" in  {
      underTest ! protocolMessage(Prepare("select * from something"))

      expectMsgPF() {
        case ProtocolResponse(_, Prepared(_, PreparedMetadata(Nil, Some("keyspace"), Some("table"), Nil), NoRowMetadata)) => true
      }
    }

    "return prepared message on prepare - single param" in {
      underTest ! protocolMessage(Prepare("select * from something where name = ?"))

      expectMsgPF() {
        case ProtocolResponse(_, Prepared(_, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
        List(ColumnSpecWithoutTable("0", Varchar))), NoRowMetadata)) => true
      }
    }

    "use types from Prime" in {
      val query = "select * from something where name = ?"
      val prepare = Prepare("select * from something where name = ?")
      val prepared = Prepared(id, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
        List(ColumnSpecWithoutTable("0", CqlInt))))

      when(primePreparedStore.apply(any(classOf[Prepare]), any[Function2[PreparedMetadata, RowMetadata, Prepared]]))
        .thenReturn(Some(Reply(prepared)))

      underTest ! protocolMessage(Prepare("select * from something where name = ?"))

      val prepareCaptor = ArgumentCaptor.forClass(classOf[Prepare])
      verify(primePreparedStore).apply(prepareCaptor.capture(), anyFunction())
      prepareCaptor.getValue() shouldEqual prepare
    }

    "use incrementing ids" in {
      var lastId: Int = -1
      for (i <- 1 to 10) {
        val query = s"select * from something where name = ? and i = $i"

        underTest ! protocolMessage(Prepare(query))

        // The id returned should always be greater than the last id returned.
        expectMsgPF() {
          case ProtocolResponse(_, Prepared(id, _, _)) if id.toInt() > lastId =>
            lastId = id.toInt()
            true
        }
      }
    }

    "record preparation in activity log" in {
      underTest ! protocolMessage(Prepare("select 1"))

      activityLogProbe.expectMsg(RecordPrepare(PreparedStatementPreparation("select 1")))
    }

    "answer queries for prepared statement - not exist" in {
      val response = (underTest ? PreparedStatementQuery(List(1))).mapTo[PreparedStatementResponse]

      Await.result(response, atMost) should equal(PreparedStatementResponse(Map()))
    }

    "answer queries for prepared statement - exists" in {
      val query = "select * from something where name = ?"
      val prepared = Prepared(id, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
        List(ColumnSpecWithoutTable("0", CqlInt))))
      when(primePreparedStore.apply(any(classOf[Prepare]), any[Function2[PreparedMetadata, RowMetadata, Prepared]]))
        .thenReturn(Some(Reply(prepared)))

      underTest ! protocolMessage(Prepare(query))
      val response = (underTest ? PreparedStatementQuery(List(id.toInt()))).mapTo[PreparedStatementResponse]
      Await.result(response, atMost) should equal(PreparedStatementResponse(Map(id.toInt() -> (query, prepared))))
    }
  }
}
