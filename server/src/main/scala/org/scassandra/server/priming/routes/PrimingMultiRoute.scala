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
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{AllPSPrimes, ClearPSPrime, GetAllPSPrimes, RecordPSPrime}
import org.scassandra.server.priming.json.PrimingJsonImplicits
import org.scassandra.server.priming.prepared._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait PrimingMultiRoute extends LazyLogging {

  import PrimingJsonImplicits._

  implicit val primePreparedMultiStore: ActorRef
  implicit val ec: ExecutionContext
  private implicit val timeout: Timeout = Timeout(250 milliseconds)

  val routeForMulti: Route =
    cors() {
      path("prime-prepared-multi") {
        post {
          entity(as[PrimePreparedMulti]) { prime: PrimePreparedMulti =>
            complete {
              logger.info(s"Received a prepared multi prime $prime")
              primePreparedMultiStore ! RecordPSPrime(prime)
              StatusCodes.OK
            }
          }
        } ~
          delete {
              onComplete(primePreparedMultiStore ? ClearPSPrime) {
                case Success(_) => complete(StatusCodes.OK)
                case Failure(t) =>
                  logger.warn("Failed to delete multi primes", t)
                  complete(StatusCodes.InternalServerError, t.getMessage)
              }
          } ~
          get {
            complete {
              (primePreparedMultiStore ? GetAllPSPrimes).mapTo[AllPSPrimes[PrimePreparedMulti]].map(_.primes)
            }
          }
      }
    }
}
