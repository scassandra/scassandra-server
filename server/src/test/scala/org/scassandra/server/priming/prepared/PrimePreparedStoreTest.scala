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

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.types.{CqlInet, CqlText, CqlVarchar}
import org.scassandra.server.priming.query.{Prime, PrimeMatch}
import org.scassandra.server.priming.{ConflictingPrimes, PrimeAddSuccess, ReadRequestTimeoutResult}
import org.scassandra.server.priming.json.ReadTimeout

import scala.concurrent.duration.FiniteDuration

class PrimePreparedStoreTest extends FunSuite with Matchers {

  test("Empty rows") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPrepared(Some(query))
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get.getPrime()   .rows should equal(List())
  }

  test("Overriding result to read_request_timeout") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPrepared(Some(query))
    val thenDo = ThenPreparedSingle(Some(List()), result = Some(ReadTimeout))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get.getPrime().result should equal(ReadRequestTimeoutResult())
  }

  test("Single row without variable type info - defaults to varchar") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPrepared(Some(query))
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val thenDo = ThenPreparedSingle(Some(rows))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(List(CqlVarchar, CqlVarchar), prime = Prime(rows, columnTypes = Map("one"-> CqlVarchar))))
  }

  test("Variable type info supplied") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPrepared(Some(query))
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val variableTypes = List(CqlInet, CqlInet)
    val thenDo = ThenPreparedSingle(Some(rows), Some(variableTypes))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(variableTypes, prime = Prime(rows, columnTypes = Map("one"-> CqlVarchar))))
  }

  test("Subset of variable type info supplied") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPrepared(Some(query))
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val variableTypes = List(CqlInet)
    val thenDo = ThenPreparedSingle(Some(rows), Some(variableTypes))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(List(CqlInet, CqlVarchar), prime = Prime(rows, columnTypes = Map("one"-> CqlVarchar))))
  }

  test("Single row with all column types") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPrepared(Some(query))
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val columnTypes = Map("one"->CqlText)
    val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(rows), column_types = Some(columnTypes))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    val result = underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    result should equal(PrimeAddSuccess)
    actualPrime.get should equal(PreparedPrime(List(CqlVarchar, CqlVarchar), prime = Prime(rows, columnTypes = columnTypes)))
  }

  test("Subset of column type info supplied - rest defaulted to varchar)") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPrepared(Some(query))
    val rows: List[Map[String, Any]] = List(Map("column_type_specified" -> "two","column_type_not_specified"->"three"))
    val columnTypes = Map("column_type_specified" -> CqlText)
    val thenDo = ThenPreparedSingle(Some(rows), column_types = Some(columnTypes))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    val expectedColumnTypes = Map("column_type_specified" -> CqlText, "column_type_not_specified" -> CqlVarchar)
    actualPrime.get should equal(PreparedPrime(List(CqlVarchar, CqlVarchar), prime = Prime(rows, columnTypes = expectedColumnTypes)))
  }

  test("Specifying result other than success") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people"
    val when = WhenPrepared(Some(query))
    val thenDo = ThenPreparedSingle(None, result = Some(ReadTimeout))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(List(), Prime(result = ReadRequestTimeoutResult())))
  }

  test("Specifying a fixed delay") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people"
    val when = WhenPrepared(Some(query))
    val thenDo = ThenPreparedSingle(rows = Some(List()), fixedDelay = Some(1500l))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(List(), Prime(fixedDelay = Some(FiniteDuration(1500l, TimeUnit.MILLISECONDS)))))
  }

  test("Clearing all the primes") {
    //given
    val underTest = new PrimePreparedStore
    val when = WhenPrepared(Some(""))
    val thenDo = ThenPreparedSingle(None)
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    underTest.clear()
    //then
    underTest.state.size should equal(0)
  }

  test("Priming consistency. Should match on consistency.") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val consistencies = List(ONE, TWO)
    val when = WhenPrepared(Some(query), None, Some(consistencies))
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    val primeForOne = underTest.findPrime(PrimeMatch(query, ONE))
    val primeForTwo = underTest.findPrime(PrimeMatch(query, TWO))
    val primeForAll = underTest.findPrime(PrimeMatch(query, ALL))
    //then
    primeForOne.isDefined should equal(true)
    primeForTwo.isDefined should equal(true)
    primeForAll.isDefined should equal(false)
  }

  test("Priming Consistency. Should default to all consistencies") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPrepared(Some(query), None)
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    val primeForOne = underTest.findPrime(PrimeMatch(query, ONE))
    val primeForTwo = underTest.findPrime(PrimeMatch(query, TWO))
    val primeForAll = underTest.findPrime(PrimeMatch(query, ALL))
    val primeForLocalOne = underTest.findPrime(PrimeMatch(query, LOCAL_ONE))
    //then
    primeForOne.isDefined should equal(true)
    primeForTwo.isDefined should equal(true)
    primeForAll.isDefined should equal(true)
    primeForLocalOne.isDefined should equal(true)
  }

  test("Conflicting primes") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val thenDo = ThenPreparedSingle(Some(List()))
    val primeForOneAndTwo = PrimePreparedSingle(WhenPrepared(Some(query), None, Some(List(ONE, TWO))), thenDo)
    val primeForTwoAndThree = PrimePreparedSingle(WhenPrepared(Some(query), None, Some(List(TWO, THREE))), thenDo)
    //when
    underTest.record(primeForOneAndTwo)
    val result = underTest.record(primeForTwoAndThree)
    //then
    result.isInstanceOf[ConflictingPrimes] should equal(true)
  }
}
