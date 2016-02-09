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

import org.scalatest.{Matchers, FunSuite}
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming.query.{Prime, PrimeMatch}

class CompositePreparedPrimeStoreTest extends FunSuite with Matchers {

  val one: PreparedStoreLookup = new PreparedStoreLookup {
    def findPrime(primeMatch: PrimeMatch): Option[PreparedPrimeResult] = None
  }
  val two: PreparedStoreLookup = new PreparedStoreLookup {
    def findPrime(primeMatch: PrimeMatch): Option[PreparedPrimeResult] = None
  }

  val result = new PreparedPrimeResult {
    def getPrime(variables: List[Any]): Prime = Prime()
    val variableTypes: List[ColumnType[_]] = List()
  }
  val three: PreparedStoreLookup = new PreparedStoreLookup {
    def findPrime(primeMatch: PrimeMatch): Option[PreparedPrimeResult] = Some(result)
  }

  test("Delegates to all stores") {
    val underTest = new CompositePreparedPrimeStore(one, two, three)
    underTest.findPrime(PrimeMatch("blah")) should equal(Some(result))
  }
}
