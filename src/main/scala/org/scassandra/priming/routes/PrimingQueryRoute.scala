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
package org.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import org.scassandra.priming._
import org.scassandra.priming.query._
import org.scassandra.priming.query.PrimeCriteria
import org.scassandra.priming.query.PrimeQuerySingle
import org.scassandra.priming.query.Prime
import scala.util.{Success, Failure}

trait PrimingQueryRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primeQueryStore: PrimeQueryStore

  val queryRoute = {
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
            val allPrimes: Map[PrimeCriteria, Prime] = primeQueryStore.getAllPrimes
            PrimeQueryResultExtractor.convertBackToPrimeQueryResult(allPrimes)
          }
        } ~
          post {
            entity(as[PrimeQuerySingle]) {
              primeRequest => {
                complete {
                  logger.debug(s"Received prime request ${primeRequest}")
                  val primeCriteriaTry = PrimeQueryResultExtractor.extractPrimeCriteria(primeRequest)

                  primeCriteriaTry match {
                    case Success(primeCriteria) =>

                      val primeResult = PrimeQueryResultExtractor.extractPrimeResult(primeRequest)

                      primeQueryStore.add(primeCriteria, primeResult) match {
                        case cp: ConflictingPrimes => StatusCodes.BadRequest -> cp
                        case tm: TypeMismatches => StatusCodes.BadRequest -> tm
                        case _ => StatusCodes.OK
                      }
                    case Failure(_) =>
                      StatusCodes.BadRequest
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
