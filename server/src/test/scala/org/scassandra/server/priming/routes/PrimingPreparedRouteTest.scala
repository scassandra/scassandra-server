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
package org.scassandra.server.priming.routes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scassandra.server.cqlmessages.types.{CqlInt, CqlVarchar}
import org.scassandra.server.cqlmessages.{ONE, TWO}
import org.scassandra.server.priming._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared.{PrimePreparedSingle, ThenPreparedSingle, WhenPrepared, _}
import org.scassandra.server.priming.query.{Prime, PrimeCriteria}
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest

class PrimingPreparedRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingPreparedRoute with MockitoSugar
 with BeforeAndAfter {

  implicit def actorRefFactory = system
  implicit val primePreparedStore: PrimePreparedStore = mock[PrimePreparedStore]
  implicit val primePreparedPatternStore: PrimePreparedPatternStore = mock[PrimePreparedPatternStore]
  implicit val primePreparedMultiStore: PreparedMultiStore = mock[PreparedMultiStore]
  val primePreparedSinglePath = "/prime-prepared-single"
  val primePreparedMultiPath = "/prime-prepared-multi"

  import PrimingJsonImplicits._

  before {
    reset(primePreparedStore)
    reset(primePreparedPatternStore)
    reset(primePreparedMultiStore)
  }

  describe("Priming multiple responses") {
    it("Should record it with the multi prime store") {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo = ThenPreparedMulti(None, List(Outcome(Criteria(List(VariableMatch(Some("Chris")))), Action(None))))
      val prime = PrimePreparedMulti(when, thenDo)
      Post(primePreparedMultiPath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedMultiStore).record(prime)
      }
    }
  }

  describe("Priming") {
    it("Should take in query") {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).record(prime)
      }
    }

    it("should store a fixedDelay") {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()), fixedDelay = Some(1500l))
      val prime = PrimePreparedSingle(when, thenDo)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).record(prime)
      }
    }

    it("Should use the pattern store if queryPattern specified") {
      val when: WhenPrepared = WhenPrepared(queryPattern = Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedPatternStore).record(prime)
      }
    }

    it("should allow primes to be deleted") {
      Delete(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).clear()
        verify(primePreparedPatternStore).clear()
      }
    }
  }

  describe("Retrieving of primes") {
    it("should return empty list when there are no primes") {
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map()
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
          responseAs[List[PrimePreparedSingle]].size should equal(0)
      }
    }

    it("should convert variable types in original Json Format") {
      val query: String = "select * from people where name = ?"
      val variableTypes = List(CqlVarchar, CqlInt)
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(variableTypes, Prime())
      )
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).thenDo.variable_types should equal(Some(variableTypes))
      }
    }

    it("should put query in original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime())
      )
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.when.query should equal(Some(query) )
      }
    }

    it("should convert rows to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val rows = List(Map("name" -> "Chris"))
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime(rows))
      )
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.rows should equal(Some(rows))
      }
    }

    it("should convert column types to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val columnTypes = Map("name" -> CqlVarchar)
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime(columnTypes = columnTypes))
      )
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).thenDo.column_types should equal(Some(columnTypes))
      }
    }

    it("should convert result to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime(result = ReadRequestTimeoutResult()))
      )
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.result should equal(Some(ReadTimeout))
      }
    }

    it("should convert consistencies to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List(ONE, TWO)) -> PreparedPrime(List(), Prime())
      )
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.when.consistency should equal(Some(List(ONE, TWO)))
      }
    }
  }

  describe("Priming errors") {
    it("Should convert Conflicting Primes to Bad Request") {
      when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(ConflictingPrimes(List()))

      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
      }
    }

    it("Should convert type mis match to Bad Request") {
      when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(TypeMismatches  (List()))

      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
      }
    }

    it("Should be Bad Request if both query and queryPattern specified") {
      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"), Some("Pattern as well"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[String] should equal("Must specify either query or queryPattern, not both")
      }
    }

    it("Should be Bad Request if neither query and queryPattern specified") {
      val primeWhen: WhenPrepared = WhenPrepared()
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[String] should equal("Must specify either query or queryPattern, not both")
      }
    }
  }

  //todo test for specifying neither a query or queryPattern

}
