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
import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{TestActor, TestProbe}
import akka.util.Timeout
import org.scalatest.{Matchers, WordSpec}
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{AllPSPrimes, ClearPSPrime, GetAllPSPrimes, RecordPSPrime}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{ConflictingPrimes, PrimeAddSuccess, TypeMismatches}
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared._

import scala.concurrent.duration._

class PrimingPreparedRouteTest extends WordSpec with Matchers with ScalatestRouteTest with PrimingPreparedRoute {

  import PrimingJsonImplicits._

  private val primePreparedSinglePath = "/prime-prepared-single"

  implicit val actorRefFactory = system
  val ec = scala.concurrent.ExecutionContext.global
  val actorTimeout: Timeout = Timeout(500 milliseconds)

  private val ppStoreProbe = TestProbe()
  private val pppStoreProbe = TestProbe()
  private val multiStoreProbe = TestProbe()

  val primePreparedStore = ppStoreProbe.ref
  val primePreparedPatternStore = pppStoreProbe.ref

  val primePreparedMultiStore: ActorRef = multiStoreProbe.ref

  "priming without a pattern" must {

    "forward to ps store" in {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      respondWith(ppStoreProbe, PrimeAddSuccess)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        ppStoreProbe.expectMsg(RecordPSPrime(prime))
      }
    }

    "forward to pattern store if queryPattern specified" in {
      val when: WhenPrepared = WhenPrepared(queryPattern = Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      respondWith(pppStoreProbe, PrimeAddSuccess)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        pppStoreProbe.expectMsg(RecordPSPrime(prime))
      }
    }

    "clear all primes on delete" in {
      respondWith(ppStoreProbe, Done)
      respondWith(pppStoreProbe, Done)
      Delete(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        ppStoreProbe.expectMsg(ClearPSPrime)
        pppStoreProbe.expectMsg(ClearPSPrime)
      }
    }
  }

  "retrieving of primes" must {
    "get primes from both stores" in {
      val when: WhenPrepared = WhenPrepared(queryPattern = Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      respondWith(pppStoreProbe, AllPSPrimes(List(prime)))
      respondWith(ppStoreProbe, AllPSPrimes(List(prime)))

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        pppStoreProbe.expectMsg(GetAllPSPrimes)
        ppStoreProbe.expectMsg(GetAllPSPrimes)
        responseAs[List[PrimePreparedSingle]] should equal(List(prime, prime))
        status should equal(StatusCodes.OK)
      }
    }

    "return 500 if can't get primes" in {
      // Prime stores won't respond
      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.InternalServerError)
      }
    }
  }

  "priming errors" must {
    val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
    val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
    val examplePrime = PrimePreparedSingle(primeWhen, thenDo)

    "convert Conflicting Primes to Bad Request" in {
      val error = ConflictingPrimes(List())
      respondWith(ppStoreProbe, error)
      Post(primePreparedSinglePath, examplePrime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[ConflictingPrimes] should equal(error)
      }
    }

    "convert type mismatch to Bad Request" in {
      //when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(TypeMismatches(List()))
      val error = TypeMismatches(List())
      respondWith(ppStoreProbe, error)
      Post(primePreparedSinglePath, examplePrime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[TypeMismatches] should equal(error)
      }
    }

    "return be bad request if both query and queryPattern specified" in {
      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"), Some("Pattern as well"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[String] should equal("Must specify either query or queryPattern")
      }
    }

    "return bad request if neither query and queryPattern specified" in {
      val primeWhen: WhenPrepared = WhenPrepared()
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[String] should equal("Must specify either query or queryPattern")
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
