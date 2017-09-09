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
package org.scassandra.server.priming.routes

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{ TestActor, TestProbe }
import akka.util.Timeout
import org.scalatest.{ Matchers, WordSpec }
import org.scassandra.codec.datatype._
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{ AllPSPrimes, ClearPSPrime, GetAllPSPrimes, RecordPSPrime }
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared._
import scala.concurrent.duration._

class PrimingMultiRouteTest extends WordSpec with Matchers with ScalatestRouteTest with PrimingMultiRoute {

  import PrimingJsonImplicits._

  implicit val actorRefFactory: ActorSystem = system
  val ec = scala.concurrent.ExecutionContext.global
  val actorTimeout: Timeout = Timeout(2 seconds)
  val multiStoreProbe = TestProbe()
  val primePreparedMultiStore: ActorRef = multiStoreProbe.ref
  val primePreparedMultiPath = "/prime-prepared-multi"

  "multi priming" must {
    "record it with the multi prime store" in {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo = ThenPreparedMulti(Some(List(Text)), List(Outcome(Criteria(List(ExactMatch(Some("Chris")))), Action(None))))
      val prime = PrimePreparedMulti(when, thenDo)
      Post(primePreparedMultiPath, prime) ~> routeForMulti ~> check {
        status should equal(StatusCodes.OK)
        multiStoreProbe.expectMsg(RecordPSPrime(prime))
      }
    }

    "clear primes on delete" in {
      respondWith(multiStoreProbe, Done)
      Delete(primePreparedMultiPath) ~> routeForMulti ~> check {
        status should equal(StatusCodes.OK)
        multiStoreProbe.expectMsg(ClearPSPrime)
      }
    }
  }

  "retrieving of multi primes" must {
    "return empty list when there are no multi primes" in {
      respondWith(multiStoreProbe, AllPSPrimes(Nil))
      Get(primePreparedMultiPath) ~> routeForMulti ~> check {
        multiStoreProbe.expectMsg(GetAllPSPrimes)
        responseAs[List[PrimePreparedMulti]] shouldEqual List.empty[PrimePreparedMulti]
      }
    }

    "return the list of multi primes" in {
      val variableMatchers: List[VariableMatch] = List(AnyMatch, ExactMatch(Some("")))
      val outcomes: List[Outcome] = List(Outcome(Criteria(variableMatchers), Action(None)))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None), ThenPreparedMulti(None, outcomes)))
      respondWith(multiStoreProbe, AllPSPrimes(existingPrimes))

      Get(primePreparedMultiPath) ~> routeForMulti ~> check {
        multiStoreProbe.expectMsg(GetAllPSPrimes)
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse should equal(existingPrimes)
      }
    }
  }

  private def respondWith(probe: TestProbe, m: Any): Unit = {
    probe.setAutoPilot((sender: ActorRef, msg: Any) => {
      msg match {
        case _ =>
          sender ! m
          TestActor.NoAutoPilot
      }
    })
  }
}
