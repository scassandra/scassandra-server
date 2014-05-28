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
package org.scassandra.priming

import akka.actor.ActorSystem
import akka.testkit._
import org.scalatest.{FunSpecLike, Matchers}
import org.scassandra.priming.query.PrimeQueryStore
import org.scassandra.priming.prepared.PrimePreparedStore
import org.scassandra.ServerReady

class PrimingServerReadyTest extends TestKit(ActorSystem("TestSystem")) with FunSpecLike with Matchers {

  describe("ServerReady") {
    it("should be send to the actor that registers as a listener") {
      // given
      // TODO [DN] Do test probes shut down their own actor systems?
      val primingReadyListener = TestProbe()

      // when
      TestActorRef(new PrimingServer(8045, PrimeQueryStore(), PrimePreparedStore(), primingReadyListener.ref))

      // then
      primingReadyListener.expectMsg(ServerReady)
    }

  }
}
