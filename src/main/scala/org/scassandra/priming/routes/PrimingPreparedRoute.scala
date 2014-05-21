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
import org.scassandra.priming.{ConflictingPrimes, TypeMismatches, PrimingJsonImplicits}
import org.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedStore, PrimePreparedSingle}
import scala.collection.immutable.Iterable

trait PrimingPreparedRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primePreparedStore : PrimePreparedStore

  val routeForPreparedPriming =
    path("prime-prepared-single") {
      post {
        entity(as[PrimePreparedSingle]) { prime =>
          complete {
            primePreparedStore.record(prime) match {
              case cp: ConflictingPrimes => StatusCodes.BadRequest -> cp
              case tm: TypeMismatches => StatusCodes.BadRequest -> tm
              case _ => StatusCodes.OK
            }
          }
        }
      } ~
      delete {
        complete {
          primePreparedStore.clear()
          StatusCodes.OK
        }
      } ~
      get {
        complete {
          val preparedPrimes: Iterable[PrimePreparedSingle] = primePreparedStore.retrievePrimes().map({case (primeCriteria, preparedPrime) =>
            PrimePreparedSingle(
              WhenPreparedSingle(primeCriteria.query, Some(primeCriteria.consistency)),
              ThenPreparedSingle(
                Some(preparedPrime.prime.rows),
                Some(preparedPrime.variableTypes),
                Some(preparedPrime.prime.columnTypes),
                Some(preparedPrime.prime.result)
              )
            )
          })
          preparedPrimes
        }
      }
    }
}
