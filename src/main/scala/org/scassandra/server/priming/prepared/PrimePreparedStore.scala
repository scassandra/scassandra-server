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

import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming._
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, PrimeMatch}
import org.scassandra.server.priming.routes.PrimeQueryResultExtractor

import scala.concurrent.duration.FiniteDuration

class PrimePreparedStore extends Logging with PreparedStore with PreparedStoreLookup {

  val validator: PrimeValidator = PrimeValidator()

  def record(prime: PrimePreparedSingle) : PrimeAddResult= {
    val then: ThenPreparedSingle = prime.then
    val rows = then.rows.getOrElse(List())
    val query = prime.when.query
    val numberOfParameters = query.get.toCharArray.count(_ == '?')
    val fixedDelay: Option[FiniteDuration] = then.fixedDelay.map(FiniteDuration(_, TimeUnit.MILLISECONDS))
    val result = PrimeQueryResultExtractor.convertToPrimeResult(then.config.getOrElse(Map()), then.result.getOrElse(Success))

    val variableTypesDefaultedToVarchar: List[ColumnType[_]] = Defaulter.defaultVariableTypesToVarChar(numberOfParameters, then.variable_types)
    val colTypes = Defaulter.defaultColumnTypesToVarchar(then.column_types, rows)

    //todo errors
    val primeToStore: PreparedPrime = PreparedPrime(variableTypesDefaultedToVarchar, prime = Prime(rows, columnTypes = colTypes,
      result = result, fixedDelay = fixedDelay))

    val consistencies = prime.when.consistency.getOrElse(Consistency.all)
    val primeCriteria = PrimeCriteria(query.get, consistencies)

    validator.validate(primeCriteria, primeToStore.prime, state.map( existingPrime => (existingPrime._1, existingPrime._2.prime)  ) ) match {
      case PrimeAddSuccess => {
        logger.info(s"Storing prime for prepared statement $primeToStore with prime criteria $primeCriteria")
        state += (primeCriteria -> primeToStore)
        PrimeAddSuccess
      }
      case notSuccess: PrimeAddResult => {
        logger.info(s"Storing prime for prepared statement $primeToStore failed due to $notSuccess")
        notSuccess
      }
    }
  }

  def findPrime(primeMatch : PrimeMatch) : Option[PreparedPrime] = {
    def findPrime: ((PrimeCriteria, PreparedPrime)) => Boolean = {
      entry => entry._1.query == primeMatch.query &&
        entry._1.consistency.contains(primeMatch.consistency)
    }
    state.find(findPrime).map(_._2)
  }
}

object PrimePreparedStore {
  def apply() = {
    new PrimePreparedStore()
  }
}
