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

import akka.actor.{ActorRef, Props}
import akka.testkit.TestActor.KeepRunning
import akka.testkit.{ImplicitSender, TestProbe}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scassandra.codec._
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages._
import org.scassandra.server.actors.Activity.BatchExecution
import org.scassandra.server.actors.ActivityLogActor.RecordBatch
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.actors.priming.PrimeBatchStoreActor.MatchResult
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.language.postfixOps

class BatchHandlerTest extends WordSpec with ProtocolActorTest with TestKitWithShutdown with ImplicitSender
  with Matchers with BeforeAndAfter with LazyLogging {

  var underTest: ActorRef = _
  var prepareHandlerProbe: TestProbe = _
  val activityLogProbe = TestProbe()
  val activityLog = activityLogProbe.ref
  val primeBatchStoreProbe = TestProbe()
  val primeBatchStore = primeBatchStoreProbe.ref

  primeBatchStoreProbe.setAutoPilot((sender: ActorRef, msg: Any) => {
    logger.error("Got msg: " + msg)
    msg match {
      case _ => sender ! MatchResult(None)
    }
    KeepRunning
  })

  before {
    prepareHandlerProbe = TestProbe()
    underTest = system.actorOf(Props(classOf[BatchHandler], activityLog, prepareHandlerProbe.ref, primeBatchStore))
    activityLogProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
  }

  "batch handler" must {
    "send back void response by default" in {
      val batch = Batch(BatchType.UNLOGGED, List(
        SimpleBatchQuery("insert into something"), SimpleBatchQuery("insert into something else")
      ))

      underTest ! protocolMessage(batch)

      expectMsgPF() {
        case ProtocolResponse(_, VoidResult) => true
      }
    }

    "record batch statement with ActivityLog" in {
      val batch = Batch(BatchType.LOGGED, List(
        SimpleBatchQuery("insert into something"),
        SimpleBatchQuery("insert into something else")
      ), serialConsistency = Some(Consistency.LOCAL_SERIAL), timestamp = Some(8675309))

      underTest ! protocolMessage(batch)

      expectMsgPF() {
        case ProtocolResponse(_, VoidResult) => true
      }

      activityLogProbe.expectMsg(RecordBatch(
        Activity.BatchExecution(List(
          Activity.BatchQuery("insert into something", BatchQueryKind.Simple),
          Activity.BatchQuery("insert into something else", BatchQueryKind.Simple)
        ), Consistency.ONE, Some(Consistency.LOCAL_SERIAL), BatchType.LOGGED, Some(8675309)))
      )
    }

    "record batch statement with ActivityLog - prepared statements" in {
      val id = 1
      val idBytes = ByteVector(id)

      implicit val protocolVersion: ProtocolVersion = ProtocolVersion.latest

      val batch = Batch(BatchType.LOGGED, List(
        PreparedBatchQuery(idBytes, List(QueryValue(1, CqlInt).value))
      ))

      underTest ! protocolMessage(batch)

      prepareHandlerProbe.expectMsg(PreparedStatementQuery(List(id)))

      val prepared = Prepared(
        idBytes,
        PreparedMetadata(Nil, Some("keyspace"), Some("table"), List(ColumnSpecWithoutTable("0", CqlInt))))

      prepareHandlerProbe.reply(PreparedStatementResponse(Map(id -> ("insert into something", prepared))))

      expectMsgPF() {
        case ProtocolResponse(_, VoidResult) => true
      }

      activityLogProbe.expectMsg(RecordBatch(
        BatchExecution(List(
          Activity.BatchQuery("insert into something", BatchQueryKind.Prepared, List(1), List(CqlInt))
        ), Consistency.ONE, None, BatchType.LOGGED, None))
      )
    }

    "record batch statement with ActivityLog - prepared statement not exist" in {
      val id = 1
      val idBytes = ByteVector(id)

      val batch = Batch(BatchType.LOGGED, List(
        PreparedBatchQuery(idBytes)
      ))

      underTest ! protocolMessage(batch)

      prepareHandlerProbe.expectMsg(PreparedStatementQuery(List(1)))
      prepareHandlerProbe.reply(PreparedStatementResponse(Map()))

      expectMsgPF() {
        case ProtocolResponse(_, VoidResult) => true
      }

      activityLogProbe.expectMsg(RecordBatch(
        BatchExecution(List(
          Activity.BatchQuery("A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?", BatchQueryKind.Prepared)
        ), Consistency.ONE, None, BatchType.LOGGED, None))
      )
    }
  }
}
