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
package org.scassandra.server.priming.routes

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestProbe
import org.scalatest.{Matchers, WordSpec}
import org.scassandra.server.actors.priming.PrimeBatchStoreActor.{BatchPrimeSingle, BatchWhen, ClearPrimes, RecordBatchPrime}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.Then
import org.scassandra.server.priming.json.{PrimingJsonImplicits, Success}
import org.scassandra.server.actors._

class PrimingBatchRouteTest extends WordSpec with Matchers with ScalatestRouteTest with PrimingBatchRoute {

  import PrimingJsonImplicits._

  implicit def actorRefFactory: ActorSystem = system

  val testProbe = TestProbe()
  val primeBatchStore: ActorRef = testProbe.ref

  private val primeBatchSinglePath = "/prime-batch-single"

  "Priming batch route" must {
    "store the prime in the prime store" in {
      val when = BatchWhen(List())
      val thenDo = Then(rows = Some(List()), result = Some(Success))
      val prime = BatchPrimeSingle(when, thenDo)
      Post(primeBatchSinglePath, prime) ~> batchRoute ~> check {
        status should equal(StatusCodes.OK)
        testProbe.expectMsg(RecordBatchPrime(prime))
      }
    }

    "allow primes to be deleted" in {
      respondWith(testProbe, Done)
      Delete(primeBatchSinglePath) ~> batchRoute ~> check {
        status should equal(StatusCodes.OK)
        testProbe.expectMsg(ClearPrimes)
      }
    }
  }
}
