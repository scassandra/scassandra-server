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

/*object PreparedPrime {
  def apply(variableTypes: List[ColumnType[_]] = List(), prime: Prime = Prime()) = new PreparedPrime(variableTypes, prime)
}*/

class PreparedMultiPrime(val variableTypes: List[ColumnType[_]], f: (List[Any] => Prime)) extends PreparedPrimeResult {
  def getPrime(variables: List[Any]): Prime = f(variables)

  override def toString = s"PreparedMultiPrime(variableTypes=$variableTypes, outcomes=$f)"
}

object PreparedMultiPrime {
  def apply(variableTypes: List[ColumnType[_]], f: (List[Any] => Prime)): PreparedMultiPrime = {
    new PreparedMultiPrime(variableTypes, f)
  }
}