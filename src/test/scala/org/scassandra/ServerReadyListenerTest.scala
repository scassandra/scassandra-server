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
package org.scassandra

import akka.actor.ActorSystem
import akka.testkit.{EventFilter, TestActorRef, TestProbe, TestKit}
import org.scalatest.{FunSpecLike, Matchers}

class ServerReadyListenerTest extends TestKit(ActorSystem("TestSystem")) with FunSpecLike with Matchers {

  describe("ServerReady") {
    it("should be proxied to the actor that registers using OnServerReady") {
      // given
      val serverReadyReceiver = TestProbe()
      val underTest = TestActorRef(new ServerReadyListener())

      serverReadyReceiver.send(underTest, OnServerReady)

      // when
      underTest ! ServerReady

      // then
      serverReadyReceiver.expectMsg(ServerReady)
    }

    it("should not fail if OnServerReady not previously called") {
      val underTest = TestActorRef(new ServerReadyListener())

      try {
        underTest.receive(ServerReady)
      } catch {
        case _: Throwable => fail("No throwable should be thrown")
      }
    }

    ignore("should log if unknown message sent") {
      val underTest = TestActorRef(new ServerReadyListener())

      EventFilter.info(message = "Received unknown message blah", occurrences = 1) intercept {
        underTest ! "blah"
      }
    }
  }
}
