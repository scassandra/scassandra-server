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

import akka.actor.{ActorSystem, Props}
import akka.io.Tcp.Connected
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Matchers}
import org.scassandra.server.priming.ActivityLog
import org.scassandra.server.priming.prepared.PrimePreparedStore
import org.scassandra.server.priming.query.PrimeQueryStore

/**
 * Unfortunately this test actually binds to the port. Not found a way to
 * stub out the akka IO manager.
 */
class TcpServerTest extends TestKit(ActorSystem("Test")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  test("Should record a connection with the ActivityLog") {
    //given
    val activityLog = new ActivityLog
    val underTest = TestActorRef(new TcpServer("localhost", 8044, new PrimeQueryStore, new PrimePreparedStore, system.actorOf(Props(classOf[ServerReadyListener])), activityLog))
    //when
    underTest ! Connected(null, null)
    //then
    activityLog.retrieveConnections().size should equal(1)
  }
}
