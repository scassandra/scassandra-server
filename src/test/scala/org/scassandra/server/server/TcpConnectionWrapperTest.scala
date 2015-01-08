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

import akka.testkit.{TestProbe, TestActorRef, TestKit}
import akka.actor.ActorSystem
import org.scalatest.FunSuiteLike
import org.scassandra.server.cqlmessages.response.VoidResult
import akka.io.Tcp.Write
import org.scassandra.server.cqlmessages.VersionTwo

class TcpConnectionWrapperTest extends TestKit(ActorSystem("TestSystem")) with FunSuiteLike  {

  implicit val impProtocolVersion = VersionTwo

  test("Should forward serialised response message") {
    val testProbeForTcpConnection = TestProbe()
    val underTest = TestActorRef(new TcpConnectionWrapper(testProbeForTcpConnection.ref))
    val anyResponse: VoidResult = VoidResult(0x0.toByte)

    underTest ! anyResponse

    testProbeForTcpConnection.expectMsg(Write(anyResponse.serialize()))
  }
}
