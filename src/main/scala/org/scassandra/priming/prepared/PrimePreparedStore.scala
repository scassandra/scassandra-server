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
package org.scassandra.priming.prepared

import org.scassandra.priming.routes.PrimeQueryResultExtractor
import org.scassandra.cqlmessages.{Consistency, CqlVarchar}
import org.scassandra.priming.{PrimeAddSuccess, PrimeAddResult, PrimeValidator, Success}
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.priming.query.PrimeCriteria
import org.scassandra.priming.query.PrimeMatch
import scala.Some
import org.scassandra.priming.query.Prime

class PrimePreparedStore extends Logging {

  val validator: PrimeValidator = PrimeValidator()

  var state: Map[PrimeCriteria, PreparedPrime] = Map()

  def retrievePrimes() = state

  def record(prime: PrimePreparedSingle) : PrimeAddResult= {
    val rows = prime.then.rows.getOrElse(List())
    val query = prime.when.query
    val result = prime.then.result.getOrElse(Success)
    val numberOfParameters = query.toCharArray.filter(_ == '?').size
    val variableTypesDefaultedToVarchar = prime.then.variable_types match {
      case Some(varTypes) => {
        val defaults = (0 until numberOfParameters).map(num => CqlVarchar).toList
        varTypes ++ (defaults drop varTypes.size)
      }
      case None => {
        (0 until numberOfParameters).map(num => CqlVarchar).toList
      }
    }
    val providedColTypes = prime.then.column_types
    val colTypes = PrimeQueryResultExtractor.defaultColumnTypesToVarchar(providedColTypes, rows)
    val primeToStore: PreparedPrime = PreparedPrime(variableTypesDefaultedToVarchar, prime = Prime(rows, columnTypes = colTypes, result = result))

    val consistencies = prime.when.consistency.getOrElse(Consistency.all)
    val primeCriteria = PrimeCriteria(query, consistencies)



    validator.validate(primeCriteria, primeToStore.prime, state.map( existingPrime => (existingPrime._1, existingPrime._2.prime)  ) ) match {
      case PrimeAddSuccess => {
        logger.info(s"Storing prime for prepared statement ${primeToStore} with prime criteria ${primeCriteria}")
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

  def clear() = {
    state = Map()
  }
}

object PrimePreparedStore {
  def apply() = {
    new PrimePreparedStore()
  }
}
