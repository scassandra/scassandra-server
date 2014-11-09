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
import org.scassandra.priming._
import scala.collection.immutable.Map
import org.scassandra.cqlmessages.types.ColumnType
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

class PrimeQueryStore extends Logging {

  val validator = PrimeValidator()

  var queryPrimes = Map[PrimeCriteria, Prime]()

  var queryPatternPrimes: Map[PrimeCriteria, Prime] = Map()

  def getAllPrimes: Map[PrimeCriteria, Prime] = queryPrimes

  def add(criteria: PrimeCriteria, prime: Prime): PrimeAddResult = {
    logger.info(s"Adding prime with criteria ${criteria} and prime result ${prime}")

    validator.validate(criteria, prime, queryPrimes) match {
      case PrimeAddSuccess => {
        if (criteria.patternMatch) {
          queryPatternPrimes += (criteria -> prime)
          PrimeAddSuccess
        } else {
          queryPrimes += (criteria -> prime)
          PrimeAddSuccess
        }

      }
      case notSuccess: PrimeAddResult => {
        notSuccess
      }
    }
  }

  def get(primeMatch: PrimeMatch): Option[Prime] = {
    logger.debug("Current primes: " + queryPrimes)
    logger.debug(s"Query for |$primeMatch|")

    def findPrime: ((PrimeCriteria, Prime)) => Boolean = {
      entry => entry._1.query == primeMatch.query &&
        entry._1.consistency.contains(primeMatch.consistency)
    }

    def findPrimePattern: ((PrimeCriteria, Prime)) => Boolean = {
      entry => {
        entry._1.query.r.findFirstIn(primeMatch.query) match {
          case Some(_) => entry._1.consistency.contains(primeMatch.consistency)
          case None => false
        }
      }
    }

   queryPrimes.find(findPrime).orElse(queryPatternPrimes.find(findPrimePattern)).map(_._2)
  }

  def getPrimeCriteriaByQuery(query: String): List[PrimeCriteria] = {
    queryPrimes.keys.filter(primeCriteria => primeCriteria.query == query).toList
  }

  def clear() = {
    queryPrimes = Map()
    queryPatternPrimes = Map()
  }
}


case class PrimeCriteria(query: String, consistency: List[Consistency], patternMatch : Boolean = false)

case class PrimeMatch(query: String, consistency: Consistency = ONE)

case class Prime( rows: List[Map[String, Any]] = List(),
                  result: Result = Success,
                  columnTypes: Map[String, ColumnType[_]] = Map(),
                  keyspace: String = "",
                  table: String = "",
                  fixedDelay: Option[FiniteDuration] = None
                 )

object PrimeQueryStore {
  def apply() = {
    new PrimeQueryStore()
  }
}