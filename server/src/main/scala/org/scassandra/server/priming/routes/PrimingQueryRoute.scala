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

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming._
import org.scassandra.server.priming.json.PrimingJsonImplicits
import org.scassandra.server.priming.query.{PrimeQuerySingle, _}

trait PrimingQueryRoute extends LazyLogging {

  import PrimingJsonImplicits._

  implicit val primeQueryStore: PrimeQueryStore

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
              primeQueryStore.getAllPrimes
            }
          } ~
            post {
              entity(as[PrimeQuerySingle]) {
                primeRequest => {
                  complete {
                    logger.debug(s"Received prime request $primeRequest")
                    primeQueryStore.add(primeRequest) match {
                      case cp: ConflictingPrimes => {
                        logger.warn(s"Received invalid prime due to conflicting primes $cp")
                        StatusCodes.BadRequest -> cp
                      }
                      case tm: TypeMismatches => {
                        logger.warn(s"Received invalid prime due to type mismatch $tm")
                        StatusCodes.BadRequest -> tm
                      }
                      case b: BadCriteria => StatusCodes.BadRequest
                      case _ => StatusCodes.OK
                    }
                  }
                }
              }
            } ~
            delete {
              complete {
                logger.debug("Deleting all recorded priming")
                primeQueryStore.clear()
                logger.debug("Return 200")
                StatusCodes.OK
              }
            }
        }
    }
  }
}
