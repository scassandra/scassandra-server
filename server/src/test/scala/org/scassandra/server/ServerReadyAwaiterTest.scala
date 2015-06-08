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

package org.scassandra.server

import org.scalatest.{Matchers, FunSpecLike}
import akka.testkit.TestKit
import akka.actor.{ActorRef, Props, ActorSystem}
import akka.pattern.AskTimeoutException
import scala.concurrent._

class ServerReadyAwaiterTest extends TestKit(ActorSystem("TestSystem")) with FunSpecLike with Matchers {

  import ExecutionContext.Implicits.global

  describe("timeout") {
    it("should timeout if no server ready quickly enough") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      intercept[AskTimeoutException] {
        ServerReadyAwaiter.run(primingReadyListener, tcpReadyListener)
      }
    }

    it("should timeout if only priming ready") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      future {
        Thread.sleep(1000)
        primingReadyListener ! ServerReady
      }

      intercept[AskTimeoutException] {
        ServerReadyAwaiter.run(primingReadyListener, tcpReadyListener)
      }
    }

    it("should timeout if only tcp ready") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      future {
        Thread.sleep(1000)
        tcpReadyListener ! ServerReady
      }

      intercept[AskTimeoutException] {
        ServerReadyAwaiter.run(primingReadyListener, tcpReadyListener)
      }
    }
  }

  describe("success") {
    it("should succeed if PRIMING THEN TCP ready") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      future {
        Thread.sleep(1000)
        primingReadyListener ! ServerReady
        Thread.sleep(500)
        tcpReadyListener ! ServerReady
      }

      awaitAndFailIfThrowable(primingReadyListener, tcpReadyListener)
    }

    it("should succeed if TCP THEN PRIMING ready") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      future {
        Thread.sleep(1000)
        tcpReadyListener ! ServerReady
        Thread.sleep(500)
        primingReadyListener ! ServerReady
      }

      awaitAndFailIfThrowable(primingReadyListener, tcpReadyListener)
    }

    it("should succeed if PRIMING ALREADY READY THEN TCP ready") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      future {
        Thread.sleep(1000)
        tcpReadyListener ! ServerReady
      }

      primingReadyListener ! ServerReady
      awaitAndFailIfThrowable(primingReadyListener, tcpReadyListener)
    }

    it("should succeed if TCP ALREADY READY THEN PRIMING ready") {
      val primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))
      val tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]))

      future {
        Thread.sleep(1000)
        primingReadyListener ! ServerReady
      }

      tcpReadyListener ! ServerReady
      awaitAndFailIfThrowable(primingReadyListener, tcpReadyListener)
    }
  }

  private def awaitAndFailIfThrowable(primingReadyListener: ActorRef, tcpReadyListener: ActorRef) {
    try {
      ServerReadyAwaiter.run(primingReadyListener, tcpReadyListener)
    } catch {
      case t: Throwable => fail("No throwable should be thrown", t)
    }
  }
}
