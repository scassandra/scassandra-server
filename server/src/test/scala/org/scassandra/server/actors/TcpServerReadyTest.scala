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

import java.net.InetSocketAddress

import akka.io.Tcp.{Bind, Bound}
import akka.testkit._
import org.scalatest.{Matchers, WordSpec}
import org.scassandra.server.ServerReady
import org.scassandra.server.priming.prepared.PrimePreparedStore

class TcpServerReadyTest extends WordSpec with TestKitWithShutdown with Matchers {

  "tcp server ready" must {
    "send to the actor that registers as a listener" in {
      val tcpReadyListener = TestProbe()
      val manager = TestProbe()
      val activityLog = TestProbe()
      val primeBatchStore = TestProbe()
      val preparedStore = TestProbe()
      val remote = new InetSocketAddress("127.0.0.1", 8046)
      val primeQueryStore = TestProbe()

      val tcpServer = TestActorRef(new TcpServer("localhost", 8046, primeQueryStore.ref, preparedStore.ref, primeBatchStore.ref, tcpReadyListener.ref, activityLog.ref, Some(manager.ref)))
      manager.expectMsgType[Bind]
      manager.send(tcpServer, Bound(remote))

      tcpReadyListener.expectMsg(ServerReady)
    }
  }
}
