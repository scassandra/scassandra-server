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

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp.{Received, Write}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Matchers}
import org.scassandra.server.RegisterHandlerMessages
import org.scassandra.server.actors.MessageHelper.SimpleQuery
import org.scassandra.server.actors.NativeProtocolMessageHandler._
import org.scassandra.server.actors.OptionsHandlerMessages.OptionsMessage
import org.scassandra.server.cqlmessages.OpCodes.Query
import org.scassandra.server.cqlmessages.OpCodes._
import org.scassandra.server.cqlmessages.response.{QueryBeforeReadyMessage, Ready}
import org.scassandra.server.cqlmessages._

import scala.concurrent.duration._

class NativeProtocolMessageHandlerTest extends TestKit(ActorSystem("NativeProtocolMessageHandlerTest")) with Matchers with FunSuiteLike with BeforeAndAfter {

  var testActorRef: TestActorRef[NativeProtocolMessageHandler] = null
  var tcpConnectionTestProbe: TestProbe = null
  var queryHandlerTestProbe: TestProbe = null
  var batchHandlerTestProbe: TestProbe = null
  var registerHandlerTestProbe: TestProbe = null
  var optionsHandlerTestProbe: TestProbe = null
  var prepareHandlerTestProbe: TestProbe = null
  var executeHandlerTestProbe: TestProbe = null

  var lastMsgFactoryUsedForQuery: CqlMessageFactory = null
  var lastMsgFactoryUsedForRegister: CqlMessageFactory = null
  var lastMsgFactoryUsedForPrepare: CqlMessageFactory = null

  before {
    tcpConnectionTestProbe = TestProbe()
    queryHandlerTestProbe = TestProbe()
    registerHandlerTestProbe = TestProbe()
    prepareHandlerTestProbe = TestProbe()
    executeHandlerTestProbe = TestProbe()
    optionsHandlerTestProbe = TestProbe()
    batchHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new NativeProtocolMessageHandler(
      (_, _, msgFactory) => {
        lastMsgFactoryUsedForQuery = msgFactory
        queryHandlerTestProbe.ref
      },
      (_, _, msgFactory, _) => {
        lastMsgFactoryUsedForQuery = msgFactory
        batchHandlerTestProbe.ref
      },
      (_, _, msgFactory) => {
        lastMsgFactoryUsedForRegister = msgFactory
        registerHandlerTestProbe.ref
      },
      (_, _, msgFactory) => {
        optionsHandlerTestProbe.ref
      },
      prepareHandlerTestProbe.ref,
      executeHandlerTestProbe.ref
    ))
  }

  test("Should send ready message when startup message sent - version one") {
    implicit val protocolVersion = VersionOne
    val process = Process(opCode = Startup, stream = 0x0, messageBody = ByteString(), protocolVersion = protocolVersion.clientCode)

    val senderProbe = TestProbe()
    implicit val sender: ActorRef = senderProbe.ref

    testActorRef ! process

    senderProbe.expectMsg(Ready(0x0.toByte))
  }

  test("Should send ready message when startup message sent - version two") {
    implicit val protocolVersion = VersionTwo
    val process = Process(opCode = Startup, stream = 0x0, messageBody = ByteString(), protocolVersion = protocolVersion.clientCode)

    val senderProbe = TestProbe()
    implicit val sender: ActorRef = senderProbe.ref

    testActorRef ! process

    senderProbe.expectMsg(Ready(0x0.toByte))
  }

  test("Should send back error if query before ready message") {
    implicit val protocolVersion = VersionTwo
    val process = Process(opCode = Query, stream = 0x0, messageBody = ByteString(), protocolVersion = protocolVersion.clientCode)

    val senderProbe = TestProbe()
    implicit val sender: ActorRef = senderProbe.ref

    testActorRef ! process

    senderProbe.expectMsg(Write(QueryBeforeReadyMessage().serialize()))
  }

  test("Should do nothing if an unrecognised opcode") {
    val timeout: FiniteDuration = 1 second
    implicit val protocolVersion = VersionTwo
    val process = Process(opCode = 0x56, stream = 0x0, messageBody = ByteString(), protocolVersion = protocolVersion.clientCode)

    val senderProbe = TestProbe()
    implicit val sender: ActorRef = senderProbe.ref

    testActorRef ! process

    senderProbe.expectNoMsg(timeout)
    queryHandlerTestProbe.expectNoMsg(timeout)
  }

  test("Should forward query to a new QueryHandler - version two of protocol") {
    sendStartupMessage()
    val stream: Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0, 1, 0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes ++ queryOptions
    val messageBody = MessageHelper.createQueryMessageBody(query).toArray

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Query, stream, ByteString(messageBody), ProtocolVersion.ClientProtocolVersionTwo)

    queryHandlerTestProbe.expectMsg(org.scassandra.server.actors.QueryHandler.Query(ByteString(queryWithLengthAndOptions), stream))
    lastMsgFactoryUsedForQuery should equal(VersionTwoMessageFactory)
  }

  test("Should forward query to a new QueryHandler - version one of protocol") {
    sendStartupMessage(VersionOne)
    val stream: Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0, 1, 0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes ++ queryOptions
    val messageBody = MessageHelper.createQueryMessageBody(query).toArray

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Query, stream, ByteString(messageBody), ProtocolVersion.ClientProtocolVersionOne)

    queryHandlerTestProbe.expectMsg(org.scassandra.server.actors.QueryHandler.Query(ByteString(queryWithLengthAndOptions), stream))
    lastMsgFactoryUsedForQuery should equal(VersionOneMessageFactory)
  }

  test("Should forward register message to RegisterHandler - version two protocol") {
    sendStartupMessage()
    val stream: Byte = 1

    val registerMessage = MessageHelper.createRegisterMessage(ProtocolVersion.ClientProtocolVersionTwo, stream)
    val messageBody = MessageHelper.createRegisterMessageBody().toArray

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Register, stream, ByteString(messageBody), ProtocolVersion.ClientProtocolVersionTwo)

    registerHandlerTestProbe.expectMsg(RegisterHandlerMessages.Register(ByteString(MessageHelper.dropHeaderAndLength(registerMessage.toArray)), stream))
    lastMsgFactoryUsedForRegister should equal(VersionTwoMessageFactory)
  }

  test("Should forward register message to RegisterHandler - version one protocol") {
    sendStartupMessage(VersionOne)
    val stream: Byte = 1

    val registerMessage = MessageHelper.createRegisterMessage(ProtocolVersion.ClientProtocolVersionOne, stream)
    val messageBody = MessageHelper.createRegisterMessageBody().toArray

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Register, stream, ByteString(messageBody), ProtocolVersion.ClientProtocolVersionOne)

    registerHandlerTestProbe.expectMsg(RegisterHandlerMessages.Register(ByteString(MessageHelper.dropHeaderAndLength(registerMessage.toArray)), stream))
    lastMsgFactoryUsedForRegister should equal(VersionOneMessageFactory)
  }

  test("Should forward options to OptionsHandler - version two protocol") {
    sendStartupMessage()

    val stream: Byte = 0x04

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Options, stream, ByteString(0), ProtocolVersion.ClientProtocolVersionTwo)

    optionsHandlerTestProbe.expectMsg(OptionsMessage(stream))
  }

  test("Should forward options to OptionsHandler - version one protocol") {
    sendStartupMessage(VersionOne)

    val stream: Byte = 0x04

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Options, stream, ByteString(0), ProtocolVersion.ClientProtocolVersionOne)

    optionsHandlerTestProbe.expectMsg(OptionsMessage(stream))
  }

  test("Should forward Prepare messages to the prepare handler") {
    sendStartupMessage()
    val streamId: Byte = 0x1
    val emptyBody = ByteString()
    val sender = TestProbe()

    sender.send(testActorRef, NativeProtocolMessageHandler.Process(OpCodes.Prepare, streamId, emptyBody, ProtocolVersion.ClientProtocolVersionTwo))

    prepareHandlerTestProbe.expectMsg(PrepareHandler.Prepare(ByteString(), streamId, VersionTwoMessageFactory, sender.ref))
  }

  test("Should forward Execute messages to the execute handler") {
    sendStartupMessage()
    val streamId: Byte = 0x1
    val emptyBody = ByteString()
    val sender = TestProbe()

    sender.send(testActorRef, NativeProtocolMessageHandler.Process(OpCodes.Execute, streamId, emptyBody, ProtocolVersion.ClientProtocolVersionTwo))

    executeHandlerTestProbe.expectMsg(ExecuteHandler.Execute(ByteString(), streamId, VersionTwoMessageFactory, sender.ref))
  }

  test("Should forward Batch messages to the batch handler") {
    sendStartupMessage()
    val streamId : Byte = 0x1
    val queries = List(SimpleQuery("insert into something"), SimpleQuery("insert into something else"))
    val messageBody = MessageHelper.createBatchMessageBody(queries, TWO).toArray

    testActorRef ! NativeProtocolMessageHandler.Process(OpCodes.Batch, streamId, ByteString(messageBody), ProtocolVersion.ClientProtocolVersionTwo)

    batchHandlerTestProbe.expectMsg(BatchHandler.Execute(ByteString(messageBody), streamId))
  }

  private def sendStartupMessage(protocolVersion: ProtocolVersion = VersionTwo) = {
    val process = Process(opCode = Startup, stream = 0x0, messageBody = ByteString(), protocolVersion = protocolVersion.clientCode)
    testActorRef ! process
  }
}
