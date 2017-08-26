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
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.priming.PrimeQueryStoreActor._
import org.scassandra.server.priming.json.PrimingJsonImplicits

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait PrimingQueryRoute extends LazyLogging with SprayJsonSupport {

  import PrimingJsonImplicits._

  implicit val primeQueryStore: ActorRef
  implicit val ec: ExecutionContext

  private implicit val timeout: Timeout = Timeout(250 milliseconds)

  val queryRoute: Route = {
    cors() {
      path("prime-query-sequence") {
        post {
          complete {
            // TODO - implement multi primes
            StatusCodes.NotFound
          }
        }
      } ~
        path("prime-query-single") {
          get {
            complete {
              (primeQueryStore ? GetAllPrimes).mapTo[AllPrimes].map(_.all)
            }
          } ~
            post {
              entity(as[PrimeQuerySingle]) {
                primeRequest => {
                  onComplete((primeQueryStore ? RecordQueryPrime(primeRequest)).mapTo[PrimeAddResult]) {
                    case Success(PrimeAddSuccess) =>
                      complete(StatusCodes.OK)
                    case Success(cp: ConflictingPrimes) =>
                      logger.warn(s"Received invalid prime due to conflicting primes $cp")
                      complete(StatusCodes.BadRequest, cp)
                    case Success(tm: TypeMismatches) =>
                      logger.warn(s"Received invalid prime due to type mismatch $tm")
                      complete(StatusCodes.BadRequest, tm)
                    case Success(b: BadCriteria) =>
                      complete(StatusCodes.BadRequest, b)
                    case Failure(t) =>
                      complete(StatusCodes.InternalServerError, t.getMessage)
                  }
                }
              }
            } ~
            delete {
              complete {
                logger.debug("Deleting all recorded priming")
                primeQueryStore ! ClearQueryPrimes
                StatusCodes.OK
              }
            }
        }
    }
  }
}
