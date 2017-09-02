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

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import cats.implicits._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.priming.PrimePreparedStoreActor._
import org.scassandra.server.actors.priming.PrimeQueryStoreActor._
import org.scassandra.server.priming.json.PrimingJsonImplicits
import org.scassandra.server.priming.prepared._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait PrimingPreparedRoute extends LazyLogging {

  import PrimingJsonImplicits._

  val primePreparedStore: ActorRef
  val primePreparedPatternStore: ActorRef
  implicit val actorTimeout: Timeout
  implicit val ec: ExecutionContext

  val routeForPreparedPriming: Route =
    cors() {
      path("prime-prepared-single") {
        post {
          entity(as[PrimePreparedSingle]) { prime =>
            val storeToUse = if (prime.when.query.isDefined && prime.when.queryPattern.isEmpty) {
              Some(primePreparedStore)
            } else if (prime.when.queryPattern.isDefined && prime.when.query.isEmpty) {
              Some(primePreparedPatternStore)
            } else {
              None
            }

            val primes = storeToUse
              .map((primer: ActorRef) => (primer ? RecordPSPrime(prime)).mapTo[PrimeAddResult])
              .sequence[Future, PrimeAddResult]

            onComplete(primes) {
              case Success(Some(PrimeAddSuccess)) =>
                complete(StatusCodes.OK)
              case Success(Some(cp: ConflictingPrimes)) =>
                complete(StatusCodes.BadRequest, cp)
              case Success(Some(tm: TypeMismatches)) =>
                complete(StatusCodes.BadRequest, tm)
              case Success(None) =>
                complete(StatusCodes.BadRequest, "Must specify either query or queryPattern")
              case Failure(e) =>
                logger.warn("Failed to process prime", e)
                complete(StatusCodes.InternalServerError)
            }
          }
        } ~
          delete {
            val cleared  = List(primePreparedPatternStore ? ClearPSPrime, primePreparedStore ? ClearPSPrime).sequence[Future, Any]
            onComplete(cleared) {
              case Success(_) => complete(StatusCodes.OK)
              case Failure(t) =>
                logger.warn("Failed to clear prepared primes", t)
                complete(StatusCodes.InternalServerError, t.getMessage)
            }
          } ~
          get {
            val allPrimes = List(primePreparedPatternStore, primePreparedStore)
              .map(store => (store ? GetAllPSPrimes).mapTo[AllPSPrimes[PrimePreparedSingle]].map(_.primes))
              .sequence[Future, List[PrimePreparedSingle]]
              .map(_.flatten)

            onComplete(allPrimes) {
              case Success(primes: List[PrimePreparedSingle]) =>
                complete(StatusCodes.OK, primes)
              case Failure(e) =>
                logger.warn("Failed to get all primes", e)
                complete(StatusCodes.InternalServerError)
            }
          }
      }
    }
}
