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
    prepareHandlerProbe.reply(PreparedStatementResponse(Map(1 -> Some("insert into something"))))
    connectionTestProbe.expectMsg(cqlMessageFactory.createVoidMessage(streamId))
    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("insert into something", PreparedStatementKind)
      ), ONE, LOGGED))
    )
  }
}
