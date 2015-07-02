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
package org.scassandra.server.priming.prepared

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, PrimeMatch}
import org.scassandra.server.priming.routes.PrimeQueryResultExtractor
import org.scassandra.server.priming.{Defaulter, PrimeAddResult, PrimeAddSuccess, Success}

import scala.concurrent.duration.FiniteDuration

class PrimePreparedPatternStore extends LazyLogging with PreparedStore with PreparedStoreLookup {

  override def record(incomingPrime: PrimePreparedSingle): PrimeAddResult = {
    val primeCriteria = PrimeCriteria(incomingPrime.when.queryPattern.get, incomingPrime.when.consistency.getOrElse(Consistency.all))
    val thenDo: ThenPreparedSingle = incomingPrime.thenDo
    val rows: List[Map[String, Any]] = thenDo.rows.getOrElse(List())
    val columnTypes = Defaulter.defaultColumnTypesToVarchar(thenDo.column_types, rows)
    val result = PrimeQueryResultExtractor.convertToPrimeResult(thenDo.config.getOrElse(Map()), thenDo.result.getOrElse(Success))
    val fixedDelay = thenDo.fixedDelay.map(FiniteDuration(_, TimeUnit.MILLISECONDS))
    val prime = Prime(rows, columnTypes = columnTypes, result = result, fixedDelay = fixedDelay)
    val preparedPrime = PreparedPrime(thenDo.variable_types.getOrElse(List()), prime)
    logger.info(s"Storing prime for prepared statement $preparedPrime with prime criteria $primeCriteria")
    state += (primeCriteria -> preparedPrime)
    PrimeAddSuccess
  }

  override def findPrime(primeMatch: PrimeMatch): Option[PreparedPrime] = {
    def findWithRegex: ((PrimeCriteria, PreparedPrime)) => Boolean = {
      entry => {
        entry._1.query.r.findFirstIn(primeMatch.query) match {
          case Some(_) => entry._1.consistency.contains(primeMatch.consistency)
          case None => false
        }
      }
    }

    state.find(findWithRegex).map(_._2).map(
      variablesAndPrime => {
        val numberOfVariables = primeMatch.query.toCharArray.count(_ == '?')
        val variableTypesDefaulted = Defaulter.defaultVariableTypesToVarChar(numberOfVariables, Some(variablesAndPrime.variableTypes))
        PreparedPrime(variableTypesDefaulted, variablesAndPrime.prime)
      })
  }

}
