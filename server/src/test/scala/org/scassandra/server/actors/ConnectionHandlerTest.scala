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

import akka.actor.ActorSystem
import akka.io.Tcp.{ResumeReading, Received, Write}
import akka.testkit._
import akka.util.ByteString
import org.scalatest._
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.response._
import org.scassandra.server.actors.QueryHandler.Query
import scala.language.postfixOps
import scala.concurrent.duration._

class ConnectionHandlerTest extends TestKit(ActorSystem("ConnectionHandlerTest")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  var testActorRef : TestActorRef[ConnectionHandler] = null

  var tcpConnectionTestProbe : TestProbe = null
  var queryHandlerTestProbe : TestProbe = null
  var batchHandlerTestProbe : TestProbe = null
  var registerHandlerTestProbe : TestProbe = null
  var optionsHandlerTestProbe : TestProbe = null
  var prepareHandlerTestProbe : TestProbe = null
  var executeHandlerTestProbe : TestProbe = null

  var lastMsgFactoryUsedForQuery : CqlMessageFactory = null
  var lastMsgFactoryUsedForRegister : CqlMessageFactory = null
  var lastMsgFactoryUsedForPrepare : CqlMessageFactory = null

  before {
    tcpConnectionTestProbe = TestProbe()
    queryHandlerTestProbe = TestProbe()
    registerHandlerTestProbe = TestProbe()
    prepareHandlerTestProbe = TestProbe()
    executeHandlerTestProbe = TestProbe()
    optionsHandlerTestProbe = TestProbe()
    batchHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new ConnectionHandler(
      self,
      (_,_,msgFactory) => {
        lastMsgFactoryUsedForQuery = msgFactory
        queryHandlerTestProbe.ref
      },
      (_,_,msgFactory, _) => {
        lastMsgFactoryUsedForQuery = msgFactory
        batchHandlerTestProbe.ref
      },
      (_,_,msgFactory) => {
        lastMsgFactoryUsedForRegister = msgFactory
        registerHandlerTestProbe.ref
      },
      (_,_,msgFactory) => {
        optionsHandlerTestProbe.ref
      },
      prepareHandlerTestProbe.ref,
      executeHandlerTestProbe.ref
    ))

    // Ignore all 'ResumeReading' messages.
    ignoreMsg {
      case ResumeReading => true
      case _ => false
    }

    lastMsgFactoryUsedForQuery = null
    clear()
  }

  test("Should do nothing if not a full message") {
    val partialMessage = ByteString(
      Array[Byte](
        ProtocolVersion.ServerProtocolVersionTwo, 0x0, 0x0, OpCodes.Query, // header
        0x0, 0x0, 0x0, 0x5,  // length
        0x0 // 4 bytes missing
      )
    )

    testActorRef ! Received(partialMessage)

    queryHandlerTestProbe.expectNoMsg()
  }

  test("Should handle query message coming in two parts") {
    sendStartupMessage()
    val query = "select * from people"
    val stream : Byte = 0x05
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream)
    
    val queryMessageFirstHalf = queryMessage take 5 toArray
    val queryMessageSecondHalf = queryMessage drop 5 toArray

    testActorRef ! Received(ByteString(queryMessageFirstHalf))
    queryHandlerTestProbe.expectNoMsg()
    
    testActorRef ! Received(ByteString(queryMessageSecondHalf))
    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
  }

  test("Should handle two cql messages in the same data message") {
    val startupMessage = MessageHelper.createStartupMessage()
    val stream : Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream)

    val twoMessages: List[Byte] = startupMessage ++ queryMessage

    testActorRef ! Received(ByteString(twoMessages.toArray))

    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
  }

  test("Should send unsupported version if protocol 3+") {
    implicit val protocolVersion = VersionTwo
    val stream : Byte = 0x0 // hard coded for now
    val startupMessage = MessageHelper.createStartupMessage(VersionThree)

    testActorRef ! Received(ByteString(startupMessage.toArray))

    expectMsg(Write(UnsupportedProtocolVersion(stream).serialize()))

    case object VersionFive extends ProtocolVersion(0x5, (0x85 & 0xFF).toByte, 5)
    val v5StartupMessage = MessageHelper.createStartupMessage(VersionFive)

    testActorRef ! Received(ByteString(v5StartupMessage.toArray))

    expectMsg(Write(UnsupportedProtocolVersion(stream).serialize()))
  }

  private def sendStartupMessage(protocolVersion: ProtocolVersion = VersionTwo) = {
    val startupMessage = MessageHelper.createStartupMessage(protocolVersion)
    testActorRef ! Received(ByteString(startupMessage.toArray))
  }

  private def clear(): Unit = {
    receiveWhile(10 milliseconds) {
      case msg @ _ =>
    }
  }
}
