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
package org.scassandra.priming.query

import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.cqlmessages._
import org.scassandra.priming.{Success, Result}
import scala.collection.immutable.Map

class PrimeQueryStore extends Logging {

  val validator: PrimeValidator = PrimeValidator()

  var queryToResults: Map[PrimeCriteria, Prime] = Map()

  def getAllPrimes: Map[PrimeCriteria, Prime] = queryToResults

  def add(criteria: PrimeCriteria, prime: Prime): PrimeAddResult = {
    logger.info(s"Adding prime with criteria $criteria and prime result $prime")

    validator.validate(criteria, prime, queryToResults) match {
      case PrimeAddSuccess => {
        queryToResults += (criteria -> prime)
        PrimeAddSuccess
      }
      case notSuccess: PrimeAddResult => notSuccess
    }
  }

  def get(primeMatch: PrimeMatch): Option[Prime] = {
    logger.debug("Current primes: " + queryToResults)
    logger.debug(s"Query for |$primeMatch|")
    def findPrime: ((PrimeCriteria, Prime)) => Boolean = {
      entry => entry._1.query == primeMatch.query &&
        entry._1.consistency.contains(primeMatch.consistency)
    }
    queryToResults.find(findPrime).map(_._2)
  }

  def getPrimeCriteriaByQuery(query: String): List[PrimeCriteria] = {
    queryToResults.keys.filter(primeCriteria => primeCriteria.query == query).toList
  }

  def clear() = {
    queryToResults = Map()
  }
}


case class PrimeCriteria(query: String, consistency: List[Consistency])

case class PrimeMatch(query: String, consistency: Consistency = ONE)

case class Prime(
                  rows: List[Map[String, Any]] = List(),
                  result: Result = Success,
                  columnTypes: Map[String, ColumnType[_]] = Map(),
                  keyspace: String = "",
                  table: String = ""
                  )

object PrimeQueryStore {
  def apply() = {
    new PrimeQueryStore()
  }
}