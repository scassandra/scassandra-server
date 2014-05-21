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
package org.scassandra.priming.routes

import org.scalatest.{FunSpec, Matchers}
import spray.testkit.ScalatestRouteTest
import org.scassandra.priming.{ConflictingPrimes, TypeMismatches, ReadTimeout, PrimingJsonImplicits}
import spray.http.StatusCodes
import org.scassandra.priming.prepared._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scassandra.priming.prepared.ThenPreparedSingle
import org.scassandra.priming.prepared.WhenPreparedSingle
import scala.Some
import org.scassandra.priming.prepared.PrimePreparedSingle
import org.scassandra.cqlmessages.{ONE, TWO, CqlVarchar, CqlInt}
import org.scassandra.priming.query.{PrimeCriteria, Prime}

class PrimingPreparedRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingPreparedRoute with MockitoSugar {

  implicit def actorRefFactory = system
  implicit val primePreparedStore : PrimePreparedStore = mock[PrimePreparedStore]
  val primePreparedSinglePath = "/prime-prepared-single"

  import PrimingJsonImplicits._

  describe("Priming") {
    it("Should take in query") {
      val when: WhenPreparedSingle = WhenPreparedSingle("select * from people where name = ?")
      val then: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, then)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).record(prime)
      }
    }

    it("should allow primes to be deleted") {
      Delete(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).clear()
      }
    }
  }

  describe("Retrieving of primes") {
    it("should return empty list when there are no primes") {
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map()
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

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
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).then.variable_types should equal(Some(variableTypes))
      }
    }

    it("should put query in original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime())
      )
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).when.query should equal(query)
      }
    }

    it("should convert rows to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val rows = List(Map("name" -> "Chris"))
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime(rows))
      )
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).then.rows should equal(Some(rows))
      }
    }

    it("should convert column types to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val columnTypes = Map("name" -> CqlVarchar)
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime(columnTypes = columnTypes))
      )
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).then.column_types should equal(Some(columnTypes))
      }
    }

    it("should convert result to the original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes : Map[PrimeCriteria, PreparedPrime] = Map(
        PrimeCriteria(query, List()) -> PreparedPrime(List(), Prime(result = ReadTimeout))
      )
      when(primePreparedStore.retrievePrimes).thenReturn(existingPrimes)

      Get("/prime-prepared-single") ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse(0).then.result should equal(Some(ReadTimeout))
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
        parsedResponse(0).when.consistency should equal(Some(List(ONE, TWO)))
      }
    }
  }

  describe("Priming errors") {
    it("Should convert Conflicting Primes to Bad Request") {
      when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(ConflictingPrimes(List()))

      val primeWhen: WhenPreparedSingle = WhenPreparedSingle("select * from people where name = ?")
      val then: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, then)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
      }
    }

    it("Should convert type mis match to Bad Request") {
      when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(TypeMismatches  (List()))

      val primeWhen: WhenPreparedSingle = WhenPreparedSingle("select * from people where name = ?")
      val then: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, then)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
      }
    }
  }

}
