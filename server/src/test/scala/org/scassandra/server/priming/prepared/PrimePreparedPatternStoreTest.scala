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

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.types.{CqlInet, CqlInt, CqlVarchar}
import org.scassandra.server.priming.query.{Prime, PrimeMatch}
import org.scassandra.server.priming.WriteRequestTimeoutResult
import org.scassandra.server.priming.json.WriteTimeout

import scala.concurrent.duration.FiniteDuration

class PrimePreparedPatternStoreTest extends FunSuite with Matchers with BeforeAndAfter {

  var underTest : PrimePreparedPatternStore= _

  before {
    underTest = new PrimePreparedPatternStore
  }

  test("Should return None if pattern does not match") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), Some(List(ONE)))
    val thenDo = ThenPreparedSingle(Some(List()))
    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from users where age = '6'"))
    result.isDefined should equal(false)
  }

  test("Should record result") {
    //given
    val pattern = ".*"
    val when = WhenPreparedSingle(None, Some(pattern), Some(List(ONE)))
    val thenDo = ThenPreparedSingle(Some(List()), result = Some(WriteTimeout))
    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from users where age = '6'"))

    //then
    result.get.prime.result should equal(WriteRequestTimeoutResult())
  }

  test("Should record fixed delay") {
    //given
    val pattern = ".*"
    val when = WhenPreparedSingle(None, Some(pattern), Some(List(ONE)))
    val thenDo = ThenPreparedSingle(Some(List()), fixedDelay = Some(2000))
    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from users where age = '6'"))

    //then
    result.get.prime.fixedDelay should equal(Some(FiniteDuration(2000, TimeUnit.MILLISECONDS)))
  }

  test("Should find prime if pattern and consistency match") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE)))
    val thenDo = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = '6'", ONE))
    result.isDefined should equal(true)
    result.get should equal(PreparedPrime(List(), Prime(List())))
  }

  test("Should not find prime if pattern matches but consistency does not") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE, TWO)))
    val thenDo = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = '6'", THREE))
    result.isDefined should equal(false)
  }


  test("Defaults consistencies to all") {
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern))
    val thenDo = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    underTest.findPrime(PrimeMatch("select name from people where age = '6'", ONE)).isDefined should equal(true)
    underTest.findPrime(PrimeMatch("select name from people where age = '6'", TWO)).isDefined should equal(true)
    underTest.findPrime(PrimeMatch("select name from people where age = '6'", THREE)).isDefined should equal(true)
    underTest.findPrime(PrimeMatch("select name from people where age = '6'", QUORUM)).isDefined should equal(true)
  }

  test("Defaults column types to varchar") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE)))
    val rows: List[Map[String, String]] = List(Map(
      "col_one" -> "value"
    ))
    val thenDo = ThenPreparedSingle(Some(rows))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val expectedColumnTypes = Map("col_one" -> CqlVarchar)
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = '6'", ONE))
    result.isDefined should equal(true)
    result.get.prime.columnTypes should equal(expectedColumnTypes)
  }

  test("Defaults variable types to varchar - this is done on find, unlike the non-pattern matching version") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE)))
    val rows: List[Map[String, String]] = List()
    val thenDo = ThenPreparedSingle(Some(rows))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = ? and name = ?", ONE))
    result.get.variableTypes should equal(List(CqlVarchar, CqlVarchar))
  }

  test("Subset of variable types specified, defaults rest to varchar") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE)))
    val rows: List[Map[String, String]] = List()
    val thenDo = ThenPreparedSingle(Some(rows), Some(List(CqlInet)))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = ? and name = ?", ONE))
    result.get.variableTypes should equal(List(CqlInet, CqlVarchar))
  }

  test("All variable types specified") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE)))
    val rows: List[Map[String, String]] = List()
    val thenDo = ThenPreparedSingle(Some(rows), Some(List(CqlInet, CqlInt)))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = ? and name = ?", ONE))
    result.get.variableTypes should equal(List(CqlInet, CqlInt))
  }

  //todo validation
  //clearing

}
