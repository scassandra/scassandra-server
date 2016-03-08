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

import org.scassandra.server.priming._
import org.scassandra.server.priming.json.WriteTimeout
import org.scassandra.server.priming.query.Prime
import spray.routing.HttpService
import com.typesafe.scalalogging.LazyLogging
import spray.http.StatusCodes
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared._
import scala.collection.immutable.Iterable

trait PrimingPreparedRoute extends HttpService with LazyLogging {

  import PrimingJsonImplicits._

  implicit val primePreparedStore: PrimePreparedStore
  implicit val primePreparedPatternStore: PrimePreparedPatternStore
  implicit val primePreparedMultiStore: PrimePreparedMultiStore

  val routeForPreparedPriming =
    path ("prime-prepared-multi") {
      post {
        entity(as[PrimePreparedMulti]) { prime: PrimePreparedMulti =>
          complete {
            logger.info(s"Received a prepared multi prime $prime")
            val variableTypes = prime.thenDo.variable_types.getOrElse(List())
            val mappedOutcomes = prime.thenDo.outcomes.map { o =>
              o.copy(criteria = o.criteria.copy(variable_matcher = PrimingJsonHelper.convertTypesBasedOnCqlTypes(variableTypes, o.criteria.variable_matcher)))
            }
            val mappedPrime = prime.copy(thenDo = prime.thenDo.copy(outcomes = mappedOutcomes))
            primePreparedMultiStore.record(mappedPrime)
            StatusCodes.OK
          }
        }
      } ~
      delete {
        complete {
          primePreparedMultiStore.clear()
          StatusCodes.OK
        }
      } ~
      get {
        complete {

          val preparedPrimes: Iterable[PrimePreparedMulti] = primePreparedMultiStore.retrievePrimes().map({ case (primeCriteria, preparedPrime) =>

            val outcomes : List[Outcome] = preparedPrime.variableMatchers.map({ case (outcome) =>
              val (variableMatchers, prime) = outcome

              val criteria = Criteria(variableMatchers)

              val result = PrimingJsonHelper.convertToResultJsonRepresentation(prime.result)
              val fixedDelay = prime.fixedDelay.map(_.toMillis)
              val action = Action(Some(prime.rows), Some(prime.columnTypes), Some(result), fixedDelay)
              Outcome(criteria, action)
            })

            PrimePreparedMulti(
              WhenPrepared(
                query = Some(primeCriteria.query),
                consistency = Some(primeCriteria.consistency)
              ),
              ThenPreparedMulti(
                Some(preparedPrime.variableTypes),
                outcomes
              )
            )
          })

          preparedPrimes
        }
      }
    } ~
    path("prime-prepared-single") {
      post {
        entity(as[PrimePreparedSingle]) { prime =>
          complete {
            val storeToUse = if (prime.when.query.isDefined && prime.when.queryPattern.isEmpty) {
              Some(primePreparedStore)
            } else if (prime.when.queryPattern.isDefined && prime.when.query.isEmpty) {
              Some(primePreparedPatternStore)
            } else {
              None
            }

            storeToUse match {
              case Some(store) => store.record(prime) match {
                case cp: ConflictingPrimes => StatusCodes.BadRequest -> cp
                case tm: TypeMismatches => StatusCodes.BadRequest -> tm
                case _ => StatusCodes.OK
              }
              case None =>
                StatusCodes.BadRequest -> "Must specify either query or queryPattern, not both"
            }
          }
        }
      } ~
      delete {
        complete {
          primePreparedStore.clear()
          primePreparedPatternStore.clear()
          StatusCodes.OK
        }
      } ~
      get {
        complete {
          val preparedPrimes: Iterable[PrimePreparedSingle] = primePreparedStore.retrievePrimes().map({case (primeCriteria, preparedPrime) =>
            val fixedDelay = preparedPrime.prime.fixedDelay.map(_.toMillis)
            val result = PrimingJsonHelper.convertToResultJsonRepresentation(preparedPrime.getPrime().result)
            PrimePreparedSingle(
              WhenPrepared(
                query = Some(primeCriteria.query), consistency = Some(primeCriteria.consistency)),
              ThenPreparedSingle(
                Some(preparedPrime.getPrime().rows),
                Some(preparedPrime.variableTypes),
                Some(preparedPrime.getPrime().columnTypes),
                Some(result),
                fixedDelay
              )
            )
          })
          preparedPrimes
        }
      }
    }
}
