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

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe, TestKit}
import akka.util.ByteString
import org.scalatest.{Matchers, FunSuiteLike}
import org.scalatest.matchers.ShouldMatchers
import org.scassandra.server.{RegisterHandlerMessages, RegisterHandler}
import org.scassandra.server.cqlmessages.VersionTwoMessageFactory

class RegisterHandlerTest extends TestKit(ActorSystem("TestSystem")) with FunSuiteLike with Matchers {
  test("Should send Ready message on any Register message") {
    val senderTestProbe = TestProbe()
    val cqlMessageFactory = VersionTwoMessageFactory
    val stream : Byte = 10
    val expectedReadyMessage = cqlMessageFactory.createReadyMessage(stream)
    val underTest = TestActorRef(new RegisterHandler(senderTestProbe.ref, cqlMessageFactory))
    val registerBody = MessageHelper.createRegisterMessageBody()

    underTest ! RegisterHandlerMessages.Register(ByteString(registerBody.toArray), stream)

    senderTestProbe.expectMsg(expectedReadyMessage)
  }
}
