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
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming._
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, PrimeMatch}
import org.scassandra.server.priming.routes.PrimingJsonHelper

import scala.concurrent.duration.FiniteDuration

class PrimePreparedStore extends PreparedStore[PrimePreparedSingle, PreparedPrime] with PreparedStoreLookup with LazyLogging {

  val validator: PrimeValidator = PrimeValidator()

  def record(prime: PrimePreparedSingle) : PrimeAddResult = {
    val thenDo: ThenPreparedSingle = prime.thenDo
    val rows = thenDo.rows.getOrElse(List())
    val query = prime.when.query

    val fixedDelay: Option[FiniteDuration] = thenDo.fixedDelay.map(FiniteDuration(_, TimeUnit.MILLISECONDS))
    val result = PrimingJsonHelper.convertToPrimeResult(thenDo.config.getOrElse(Map()), thenDo.result.getOrElse(Success))

    val numberOfParameters = query.get.toCharArray.count(_ == '?')
    val variableTypesDefaultedToVarchar: List[ColumnType[_]] = Defaulter.defaultVariableTypesToVarChar(numberOfParameters, thenDo.variable_types)
    val colTypes = Defaulter.defaultColumnTypesToVarchar(thenDo.column_types, rows)

    val primeToStore: PreparedPrime = PreparedPrime(variableTypesDefaultedToVarchar, prime = Prime(rows, columnTypes = colTypes,
      result = result, fixedDelay = fixedDelay))

    val consistencies = prime.when.consistency.getOrElse(Consistency.all)
    val primeCriteria = PrimeCriteria(query.get, consistencies)

    validator.validate(primeCriteria, primeToStore.getPrime(List()), state.map( existingPrime => (existingPrime._1, existingPrime._2.getPrime(List()))  ) ) match {
      case PrimeAddSuccess =>
        logger.info(s"Storing prime for prepared statement $primeToStore with prime criteria $primeCriteria")
        state += (primeCriteria -> primeToStore)
        PrimeAddSuccess
      case notSuccess: PrimeAddResult =>
        logger.info(s"Storing prime for prepared statement $primeToStore failed due to $notSuccess")
        notSuccess
    }
  }

  def findPrime(primeMatch : PrimeMatch) : Option[PreparedPrime] = {
    def findPrime: ((PrimeCriteria, PreparedPrime)) => Boolean = {
      case (primeCriteria, _) => primeCriteria.query == primeMatch.query &&
        primeCriteria.consistency.contains(primeMatch.consistency)
    }
    state.find(findPrime).map(_._2)
  }
}

object PrimePreparedStore {
  def apply() = {
    new PrimePreparedStore()
  }
}
