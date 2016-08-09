/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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

import org.scassandra.server.priming.query.Prime
import org.scassandra.server.cqlmessages.types.ColumnType

trait PreparedPrimeResult {
  val variableTypes: List[ColumnType[_]]
  def getPrime(variables: List[Any] = List()): Prime
}

case class PreparedPrime(variableTypes: List[ColumnType[_]] = List(),
                  prime: Prime = Prime()) extends PreparedPrimeResult {
  def getPrime(variables: List[Any] = List()): Prime = prime
}

class PreparedMultiPrime(val variableTypes: List[ColumnType[_]], val variableMatchers: List[(List[VariableMatch], Prime)]) extends PreparedPrimeResult {
  def getPrime(variables: List[Any]): Prime = {
    val prime: Option[(List[VariableMatch], Prime)] = variableMatchers.find({ case (matchers, _) if matchers.size == variables.size =>
      val zipped: List[(VariableMatch, Any)] = matchers.zip(variables)
      zipped.forall({case (m, v) => m.test(v) })
    })
    // todo deal with a prime that doesn't exist
    if (prime.isDefined) {
      prime.get._2
    } else {
      // temp - maybe say the last outcome is the default or make a default mandatory?
      throw new RuntimeException(s"When using multi primes the variables must match at least one of the outcomes, variables: $variables matchers: $variableMatchers")
    }
  }

  override def toString = s"PreparedMultiPrime(variableTypes=$variableTypes, outcomes=$variableMatchers)"
}

object PreparedMultiPrime {
  def apply(variableTypes: List[ColumnType[_]], variableMatchers: List[(List[VariableMatch], Prime)]): PreparedMultiPrime = {
    new PreparedMultiPrime(variableTypes, variableMatchers)
  }
}