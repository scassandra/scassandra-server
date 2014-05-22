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

import org.scalatest._
import spray.http.StatusCodes.BadRequest
import spray.http.StatusCodes.OK
import spray.testkit.ScalatestRouteTest
import org.scassandra.cqlmessages._
import org.scassandra.priming._
import org.scassandra.priming.query._
import org.scassandra.priming.query.PrimeCriteria
import scala.Some
import org.scassandra.priming.query.PrimeMatch
import org.scassandra.priming.query.Prime


class PrimingQueryRouteTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with PrimingQueryRoute {
  
  import PrimingJsonImplicits._

  implicit def actorRefFactory = system

  implicit val primeQueryStore = PrimeQueryStore()

  val primeQuerySinglePath: String = "/prime-query-single"
  
  after {
    primeQueryStore.clear()
  }

  describe("Priming single queries") {

    it("should return all primes for get") {
      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        status should equal(OK)
      }
    }

    it("should return OK on valid request") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )


      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults)))) ~> queryRoute ~> check {
        status should equal(OK)
      }
    }

    it("should populate PrimedResults on valid request") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val defaultedColumnTypes = Map("name" -> CqlVarchar, "age" -> CqlVarchar)

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults)))) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Success, defaultedColumnTypes))
      }
    }

    it("should populate PrimedResults with ReadTimeout when result is read_request_timeout") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("read_request_timeout")

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, ReadTimeout))
      }
    }

    it("should populate PrimedResults with WriteTimeout when result is write_request_timeout") {
      val query = "insert into something"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("write_request_timeout")

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, WriteTimeout))
      }
    }

    it("should populate PrimedResults with Success for result success") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Success))
      }
    }

    it("should populate PrimedResults with Unavailable for result unavailable") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("unavailable")

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Unavailable))
      }
    }

    it("should delete all primes for a HTTP delete") {
      val whenQuery = PrimeCriteria("anything", List())
      primeQueryStore.add(whenQuery, Prime(List()))

      Delete(primeQuerySinglePath) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)) should equal(None)
      }
    }
  }

  describe("Priming incorrectly") {

    it("should reject conflicting primes as bad request") {
      val consistencies: List[Consistency] = List(ONE, TWO)
      val query: String = "select * from people"
      primeQueryStore.add(PrimeCriteria(query, consistencies), Prime(List[Map[String, Any]]()))

      val whenQuery = When("select * from people")
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[ConflictingPrimes] should equal(ConflictingPrimes(existingPrimes = List(PrimeCriteria(query, consistencies))))
      }
    }

    describe("should reject type mismatch in primes as bad request") {
      val when = When("select * from people")

      val thenResults: List[Map[String, String]] =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map("age" -> CqlBoolean)

      val then = Then(Some(thenResults), Some("success"), Some(thenColumnTypes))

      Post(primeQuerySinglePath, PrimeQuerySingle(when, then)) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[TypeMismatches] should equal(TypeMismatches(List(TypeMismatch("99", "age", "boolean"), TypeMismatch("12", "age", "boolean"))))
      }
    }
  }

  describe("Setting optional values in 'when'") {
    describe("keyspace") {
      it("should correctly populate PrimedResults with empty string if keyspace name not set") {
        val query = "select * from users"
        val whenQuery = When(query)
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore.get(PrimeMatch(query, ONE)).get
          prime.keyspace should equal("")
        }

      }

      it("should correctly populate PrimedResults if keyspace name is set") {
        val expectedKeyspace = "mykeyspace"
        val query = "select * from users"
        val whenQuery = When(query, keyspace = Some(expectedKeyspace))
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore.get(PrimeMatch(query, ONE)).get
          prime.keyspace should equal(expectedKeyspace)
        }

      }
    }

    describe("table") {
      it("should correctly populate PrimedResults with empty string if table name not set") {
        val query = "select * from users"
        val whenQuery = When(query)
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore.get(PrimeMatch(query, ONE)).get
          prime.table should equal("")
        }

      }

      it("should correctly populate PrimedResults if table name is set") {
        val expectedTable = "mytable"
        val query = "select * from users"
        val whenQuery = When(query, table = Some(expectedTable))
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore.get(PrimeMatch(query, ONE)).get
          prime.table should equal(expectedTable)
        }

      }
    }

  }

  describe("Priming of types") {
    it("Should pass column types to store") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map("age" -> CqlInt)
      val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = thenColumnTypes))
      }
    }

    it("Should pass column types to store even if there are no rows that contain them") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map("age" -> CqlInt, "abigD" -> CqlDecimal)
      val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = thenColumnTypes))
      }
    }

    it("Should handle floats as JSON numbers") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("age" -> 7.7))
      val thenRowsStorageFormat = List(Map("age" -> BigDecimal("7.7")))
      val thenColumnTypes = Map("age" -> CqlFloat)
      val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
        primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRowsStorageFormat, Success, columnTypes = thenColumnTypes))
      }
    }

    it("Should handle floats as JSON strings") {
        val query = "select * from users"
        val whenQuery = When(query)
        val thenRows = List(Map("age" -> "7.7"))
        val thenColumnTypes = Map("age" -> CqlDouble)
        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          primeQueryStore.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = thenColumnTypes))
        }
    }

  }

  describe("Retrieving of primes") {
    it("should convert a prime back to the original JSON format") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = Some(List(Map("field" -> "2c530380-b9f9-11e3-850e-338bb2a2e74f")))
      val thenColumnTypes = Some(Map("field" -> CqlTimeUUID))
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows, column_types = thenColumnTypes))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, columnTypes = thenColumnTypes)

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

    it ("should convert result back to original JSON format") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = Some(List(Map("one"->"two")))
      val result = "read_request_timeout"
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows, Some(result)))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, result = result)
      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

    it("Should return keyspace passed into prime") {
      val query = "select * from users"
      val whenQuery = When(query, keyspace = Some("myKeyspace"))
      val thenRows = Some(List(Map("one"->"two")))
      val columnTypes = Map("one"->CqlVarchar)
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows, column_types =  Some(columnTypes)))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, keyspace = "myKeyspace")

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

    it("Should return table passed into prime") {
      val query = "select * from users"
      val whenQuery = When(query, table = Some("tablename"))
      val thenRows = Some(List(Map("one"->"two")))
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, table = "tablename")

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

    it("Should return consistencies passed into prime") {
      val query = "select * from users"
      val consistencies = Some(List("ONE", "ALL"))
      val whenQuery = When(query, consistency = consistencies)
      val thenRows = Some(List(Map("one"->"two")))
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, consistencies = consistencies)

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

  }


  private def createPrimeQueryResultWithDefaults(query: String,
                                                 keyspace : String = "",
                                                 table : String = "",
                                                 thenRows : Option[List[Map[String, String]]] = None,
                                                 result : String = "success",
                                                 columnTypes : Option[Map[String, ColumnType[_]]] = None,
                                                 consistencies : Option[List[String]] = None) = {


    val consistenciesDefaultingToAll = if (consistencies.isDefined) {
      consistencies.get
    } else {
      Consistency.all.map(_.string)
    }
    val colTypesDefaultingToVarchar = if (columnTypes.isDefined) {
      columnTypes.get
    } else {
      // check that all the columns in the rows have a type
      val columnNamesInAllRows = thenRows.get.flatMap(row => row.keys).distinct
      columnNamesInAllRows.map(colName => (colName, CqlVarchar)).toMap
    }
    PrimeQuerySingle(When(query, keyspace = Some(keyspace), table = Some(table), consistency = Some(consistenciesDefaultingToAll)), Then(thenRows, Some(result), Some(colTypesDefaultingToVarchar)))
  }
}
