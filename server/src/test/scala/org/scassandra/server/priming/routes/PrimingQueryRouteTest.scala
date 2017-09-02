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
import akka.http.scaladsl.model.StatusCodes.{BadRequest, OK}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{TestActor, TestProbe}
import akka.util.Timeout
import org.scalatest._
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype._
import org.scassandra.codec.{Query => CQuery}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor._
import org.scassandra.server.priming.json._

import scala.concurrent.duration._

class PrimingQueryRouteTest extends WordSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with PrimingQueryRoute {

  import PrimingJsonImplicits._

  implicit def actorRefFactory: ActorSystem = system
  val ec = scala.concurrent.ExecutionContext.global
  val actorTimeout: Timeout = Timeout(2 seconds)

  private val primeQueryStoreProbe = TestProbe()
  val primeQueryStore = primeQueryStoreProbe.ref

  private val primeQuerySinglePath: String = "/prime-query-single"
  private val exampleQuery: String = "select * from users"
  private val exampleWhen = When(query = Some(exampleQuery))
  private val exampleThen =
    List(
      Map( "name" -> "Mickey", "age" -> "99")
    )
  val examplePrime = PrimeQuerySingle(exampleWhen, Then(Some(exampleThen)))

  "prime query route" must {
    "return all primes for get" in {
      respondWith(primeQueryStoreProbe, AllPrimes(List(examplePrime)))
      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        primeQueryStoreProbe.expectMsg(GetAllPrimes)
        responseAs[List[PrimeQuerySingle]] should equal(List(examplePrime))
        status should equal(OK)
      }
    }

    "create prime with store on valid request" in {
      val whenQuery = When(query = Some(exampleQuery))
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults)))
      respondWith(primeQueryStoreProbe, PrimeAddSuccess)

      Post(primeQuerySinglePath, prime) ~> queryRoute ~> check {
        primeQueryStoreProbe.expectMsg(RecordQueryPrime(prime))
        status should equal(OK)
      }
    }

    "allow creating of read timeout primes" in {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults = List[Map[String, String]]()
      val result = Some(ReadTimeout)
      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))
      respondWith(primeQueryStoreProbe, PrimeAddSuccess)

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        primeQueryStoreProbe.expectMsg(RecordQueryPrime(prime))
        status should equal(OK)
      }
    }

    "delete all primes for http delete" in {
      respondWith(primeQueryStoreProbe, Done)
      Delete(primeQuerySinglePath) ~> queryRoute ~> check {
        primeQueryStoreProbe.expectMsg(ClearQueryPrimes)
        status should equal(OK)
      }
    }

    "accept a map as a column type" in {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults =
        List(
          Map("mapValue" -> Map())
        )
      val columnTypes = Some(Map[String, DataType]("mapValue" -> CqlMap(Varchar, Varchar)))
      respondWith(primeQueryStoreProbe, PrimeAddSuccess)
      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults), column_types = columnTypes))

      Post(primeQuerySinglePath, prime) ~> queryRoute ~> check {
        status should equal(OK)
        primeQueryStoreProbe.expectMsg(RecordQueryPrime(prime))
      }
    }
  }

  "Priming with a queryPattern" must {
    "should accept a prime with a queryPattern" in {
      val query = "select \\* from users"
      val whenQuery = When(queryPattern = Some(query))
      val thenResults =
        List(
          Map("name" -> "Mickey", "age" -> "99")
        )
      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults)))
      respondWith(primeQueryStoreProbe, PrimeAddSuccess)

      Post(primeQuerySinglePath, prime) ~> queryRoute ~> check {
        primeQueryStoreProbe.expectMsg(RecordQueryPrime(prime))
        status should equal(OK)
      }
    }
  }

  "Priming incorrectly" must {

    "reject conflicting primes as bad request" in {
      val consistencies: List[Consistency] = List(ONE, TWO)
      val query: String = "select * from people"
      val whenQuery = When(query = Some("select * from people"))
      val thenResults = List[Map[String, String]]()
      val result = Some(Success)
      val error = ConflictingPrimes(existingPrimes = List(PrimeCriteria(query, consistencies)))
      respondWith(primeQueryStoreProbe, error)

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[ConflictingPrimes] should equal(error)
      }
    }

    "reject type mismatch as bad request" in {
      val when = When(query = Some("select * from people"))
      val thenResults: List[Map[String, String]] = List()
      val thenDo = Then(Some(thenResults), Some(Success))
      val error = TypeMismatches(List())
      respondWith(primeQueryStoreProbe, error)

      Post(primeQuerySinglePath, PrimeQuerySingle(when, thenDo)) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[TypeMismatches] should equal(TypeMismatches(List()))
      }
    }

    "reject bat criteria as bad request" in {
      val when = When(query = Some("select * from people"))
      val thenResults: List[Map[String, String]] = List()
      val thenDo = Then(Some(thenResults), Some(Success))
      val error = BadCriteria("cats")
      respondWith(primeQueryStoreProbe, error)

      Post(primeQuerySinglePath, PrimeQuerySingle(when, thenDo)) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[BadCriteria] should equal(BadCriteria("cats"))
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
