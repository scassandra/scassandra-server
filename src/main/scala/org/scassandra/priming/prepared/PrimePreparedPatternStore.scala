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

import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.cqlmessages.Consistency
import org.scassandra.priming.{Defaulter, PrimeAddSuccess, PrimeAddResult}
import org.scassandra.priming.query.{Prime, PrimeCriteria, PrimeMatch}
import scala.util.matching.Regex

class PrimePreparedPatternStore extends Logging with PreparedStore with PreparedStoreLookup {

  override def record(incomingPrime: PrimePreparedSingle): PrimeAddResult = {
    val primeCriteria = PrimeCriteria(incomingPrime.when.queryPattern.get, incomingPrime.when.consistency.getOrElse(Consistency.all))
    val rows: List[Map[String, Any]] = incomingPrime.then.rows.getOrElse(List())
    val columnTypes = Defaulter.defaultColumnTypesToVarchar(incomingPrime.then.column_types, rows)
    val prime = Prime(rows, columnTypes = columnTypes)
    val preparedPrime = PreparedPrime(incomingPrime.then.variable_types.getOrElse(List()), prime)
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
        val numberOfVariables = primeMatch.query.toCharArray.filter(_ == '?').size
        val variableTypesDefaulted = Defaulter.defaultVariableTypesToVarChar(numberOfVariables, Some(variablesAndPrime.variableTypes))
        PreparedPrime(variableTypesDefaulted, variablesAndPrime.prime)
      })
  }

}
