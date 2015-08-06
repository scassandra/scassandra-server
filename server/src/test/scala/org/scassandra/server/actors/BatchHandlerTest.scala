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

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestKit}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, Matchers, FunSuiteLike}
import org.scassandra.server.actors.MessageHelper._
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.priming.{BatchQuery, BatchExecution, ActivityLog}

class BatchHandlerTest extends TestKit(ActorSystem("BatchHandlerTest")) with FunSuiteLike
  with Matchers with BeforeAndAfter {

  var underTest: ActorRef = _
  var connectionTestProbe: TestProbe = _
  var prepareHandlerProbe: TestProbe = _
  val cqlMessageFactory = VersionTwoMessageFactory
  val activityLog = new ActivityLog

  before {
    connectionTestProbe = TestProbe()
    prepareHandlerProbe = TestProbe()
    underTest = system.actorOf(Props(classOf[BatchHandler], connectionTestProbe.ref,
      cqlMessageFactory, activityLog, prepareHandlerProbe.ref))
    activityLog.clearBatchExecutions()
  }

  val streamId: Byte = 0x01

  test("Should send back void response by default") {
    val batchMessage: Array[Byte] = dropHeaderAndLength(createBatchMessage(
      List(SimpleQuery("insert into something"), SimpleQuery("insert into something else")),
      streamId))

    underTest ! BatchHandler.Execute(ByteString(batchMessage), streamId)

    connectionTestProbe.expectMsg(cqlMessageFactory.createVoidMessage(streamId))
  }

  test("Records batch statement with ActivityLog") {
    val batchMessage: Array[Byte] = dropHeaderAndLength(createBatchMessage(
      List(SimpleQuery("insert into something"), SimpleQuery("insert into something else")),
      streamId))

    underTest ! BatchHandler.Execute(ByteString(batchMessage), streamId)

    connectionTestProbe.expectMsg(cqlMessageFactory.createVoidMessage(streamId))
    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("insert into something", QueryKind),
        BatchQuery("insert into something else", QueryKind)
      ), ONE, LOGGED))
    )
  }

  test("Records batch statement with ActivityLog - prepared statements") {
    val batchMessage: Array[Byte] = dropHeaderAndLength(createBatchMessage(
      List(PreparedStatement(1)),
      streamId))

    underTest ! BatchHandler.Execute(ByteString(batchMessage), streamId)

    prepareHandlerProbe.expectMsg(PreparedStatementQuery(List(1)))
    prepareHandlerProbe.reply(PreparedStatementResponse(Map(1 -> "insert into something")))
    connectionTestProbe.expectMsg(cqlMessageFactory.createVoidMessage(streamId))
    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("insert into something", PreparedStatementKind)
      ), ONE, LOGGED))
    )
  }

  // this isn't expected to happen byt let's do something better than a NoSuchElementException if it does
  test("Records batch statement with ActivityLog - prepared statement not exist") {
    val batchMessage: Array[Byte] = dropHeaderAndLength(createBatchMessage(
      List(PreparedStatement(1)),
      streamId))

    underTest ! BatchHandler.Execute(ByteString(batchMessage), streamId)

    prepareHandlerProbe.expectMsg(PreparedStatementQuery(List(1)))
    prepareHandlerProbe.reply(PreparedStatementResponse(Map()))
    connectionTestProbe.expectMsg(cqlMessageFactory.createVoidMessage(streamId))
    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?", PreparedStatementKind)
      ), ONE, LOGGED))
    )
  }
}
