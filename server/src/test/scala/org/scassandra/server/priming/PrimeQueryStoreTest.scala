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
package org.scassandra.server.priming

import java.util.UUID

import org.scalatest.{FunSpec, Matchers}
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.messages.ColumnSpec.column
import org.scassandra.codec.messages._
import org.scassandra.codec.{Consistency, Rows, SetKeyspace, Query => CQuery}
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query._
import org.scassandra.server.priming.routes.PrimingJsonHelper
import org.scassandra.codec.datatype._
import org.scassandra.cql.CqlTimestamp

class PrimeQueryStoreTest extends FunSpec with Matchers {

  // An example prime reused throughout tests.
  val someQuery = "some query"
  val someThen = Then(
    rows = Some(List(
      Map("name" -> "Mickey", "age" -> 99),
      Map("name" -> "Mario", "age" -> 12)
    )),
    result = Some(Success),
    column_types = Some(Map("name" -> Varchar, "age" -> CqlInt))
  )
  val somePrime = PrimeQuerySingle(
    When(
      Some(someQuery),
      consistency = Some(List(Consistency.ONE))
    ),
    someThen
  )

  def withConsistency(consistency: Consistency*): PrimeQuerySingle = {
    somePrime.copy(when = somePrime.when.copy(consistency = Some(consistency.toList)))
  }

  // What we expect the Prime message to be.
  val someRows = Rows(
    RowMetadata(keyspace = Some(""), table = Some(""), columnSpec = Some(List(column("name", Varchar), column("age", CqlInt)))),
    List(Row("name" -> "Mickey", "age" -> 99), Row("name" -> "Mario", "age" -> 12))
  )

  describe("add() and apply()") {
    it("should add so that it can be retrieved using get") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      primeResults.add(somePrime)
      val actualResult = primeResults(CQuery(someQuery, QueryParameters(consistency=Consistency.ONE)))

      // then
      actualResult should matchPattern { case Some(Reply(`someRows`, _, _)) => }
    }
  }

  describe("apply()") {
    it("should return Nil if no results for given query") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      val actualResult = primeResults(CQuery("some query"))

      // then
      actualResult.isEmpty should equal(true)
    }

    it("should return SetKeyspace for 'use' queries") {
      // given
      val primeResults = new PrimeQueryStore
      val keyspace = "catbus"


      val prefixes = List("use", " USE", "uSE    ")
      prefixes.foreach { prefix =>
        // when
        val actualResult = primeResults(CQuery(s"$prefix $keyspace"))

        // then
        actualResult should matchPattern { case Some(Reply(SetKeyspace(`keyspace`), _, _)) => }
      }
    }
  }

  describe("clear()") {
    it("should clear all results") {
      // given
      val primeResults = new PrimeQueryStore
      val queryText = "select * from users"

      // when
      primeResults.add(somePrime)
      primeResults.add(PrimeQuerySingle(When(queryPattern = Some("select .*")), someThen))
      primeResults.clear()
      val primes = primeResults(CQuery(queryText))

      // then
      primes.isEmpty should equal(true)
    }
  }

  describe("add() with specific consistency") {
    it("should only return if consistency matches - single") {
      // given
      val primeResults = new PrimeQueryStore
      // when
      primeResults add withConsistency(Consistency.ONE)
      val actualResult = primeResults(CQuery(someQuery))

      // then
      actualResult should matchPattern { case Some(Reply(`someRows`, _, _)) => }
    }

    it("should not return if consistency does not match - single") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      primeResults add withConsistency(Consistency.TWO)
      val actualResult = primeResults(CQuery(someQuery))

      // then
      actualResult.isEmpty
    }

    it("should not return if consistency does not match - many") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      primeResults add withConsistency(Consistency.ANY, Consistency.TWO)
      val actualResult = primeResults(CQuery(someQuery))

      // then
      actualResult.isEmpty
    }

    it("should throw something if primes overlap partially") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      val primeForTwoAndAny = withConsistency(Consistency.TWO, Consistency.ANY)
      val primeForThreeAndAny = withConsistency(Consistency.THREE, Consistency.ANY)
      primeResults.add(primeForTwoAndAny)

      // then
      val primeAddResult = primeResults.add(primeForThreeAndAny)
      primeAddResult should equal (ConflictingPrimes(List(PrimingJsonHelper.extractPrimeCriteria(primeForTwoAndAny).get)))
    }

    it("should override if it is the same prime criteria") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      val primeForTwoAndAny = withConsistency(Consistency.TWO, Consistency.ANY)
      val primeForTwoAndAnyAgain = primeForTwoAndAny.copy(thenDo = primeForTwoAndAny.thenDo.copy(rows = None))
      primeResults.add(primeForTwoAndAny)

      // add a second time with the same criteria.
      primeResults.add(primeForTwoAndAnyAgain)

      // then - results should be from the second add.
      val actualResult = primeResults(CQuery(someQuery, QueryParameters(consistency = Consistency.ANY)))
      actualResult.get should equal (primeForTwoAndAnyAgain.prime)
    }

    it("should allow many primes for the same criteria if consistency is different") {
      // given
      val primeResults = new PrimeQueryStore

      // when
      val primeCriteriaForONE = withConsistency(Consistency.ONE)
      val primeCriteriaForTWO = withConsistency(Consistency.TWO)
      val primeCriteriaForTHREE = withConsistency(Consistency.THREE)

      // then - each add should be successful as there there are no conflicts as consistencies are different.
      primeResults.add(primeCriteriaForONE) shouldEqual PrimeAddSuccess
      primeResults.add(primeCriteriaForTWO) shouldEqual PrimeAddSuccess
      primeResults.add(primeCriteriaForTHREE) shouldEqual PrimeAddSuccess

      primeResults.getAllPrimes.size shouldEqual 3
    }
  }

  describe("apply() for query patterns") {
    it("should treat .* as a wild card") {
      //given
      val primeQueryStore = new PrimeQueryStore

      //when
      primeQueryStore.add(PrimeQuerySingle(When(queryPattern = Some(".*")), someThen))
      val primeResults: Option[Prime] = primeQueryStore(CQuery("select anything"))

      //then
      primeResults.isDefined should equal(true)
    }

    it("should treat .+ as a wild card - no match") {
      //given
      val primeQueryStore = new PrimeQueryStore

      //when
      primeQueryStore.add(PrimeQuerySingle(When(queryPattern = Some("hello .+")), someThen))
      val primeResults: Option[Prime] = primeQueryStore(CQuery("hello "))

      //then
      primeResults.isDefined should equal(false)
    }

    it("should treat .+ as a wild card - match") {
      //given
      val primeQueryStore = new PrimeQueryStore

      //when
      primeQueryStore.add(PrimeQuerySingle(When(queryPattern = Some("hello .+")), someThen))
      val primeResults: Option[Prime] = primeQueryStore(CQuery("hello a"))

      //then
      primeResults.isDefined should equal(true)
    }
  }

  describe("add() with type mismatch should return validation errors") {

    it("when column value not Int") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 123),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT AN INTEGER!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlInt))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT AN INTEGER!", "hasInvalidValue", CqlInt.stringRep))))
    }

    it("when column value Int as BigDecimal") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("hasLongAsInt" -> BigDecimal("5"))
          )),
          result = Some(Success),
          column_types = Some(Map("hasLongAsInt" -> CqlInt))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(PrimeAddSuccess)
    }

    it("when column value not Boolean") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "true"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A BOOLEAN!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlBoolean))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A BOOLEAN!", "hasInvalidValue", CqlBoolean.stringRep))))
    }

    it("when column value not Bigint") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "12345"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A BIGINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Bigint))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A BIGINT!", "hasInvalidValue", Bigint.stringRep))))
    }

    it("when column value not Counter") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "1234"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A COUNTER!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Counter))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A COUNTER!", "hasInvalidValue", Counter.stringRep))))
    }

    it("when column value not Blob") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "0x48656c6c6f"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Blob))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", Blob.stringRep))))
    }

    it("when column value not Decimal") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlDecimal))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", CqlDecimal.stringRep))))
    }

    it("when column value not Double") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlDouble))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", CqlDouble.stringRep))))
    }

    it("when column value not Float") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlFloat))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", CqlFloat.stringRep))))
    }

    it("when column value not Timestamp") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "1368438171000"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIMESTAMP!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Timestamp))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TIMESTAMP!", "hasInvalidValue", Timestamp.stringRep))))
    }

    it("when column value not Uuid") {
      // given
      val uuid = UUID.randomUUID().toString
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> uuid),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Uuid))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", Uuid.stringRep))))
    }

    it("when column value not Inet") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "127.0.0.1"),
            Map("name" -> "validIpv6", "hasInvalidValue" -> "::1"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT AN INET!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlInet))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT AN INET!", "hasInvalidValue", CqlInet.stringRep))))
    }

    it("when column value not Varint") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "1234"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A VARINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Varint))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A VARINT!", "hasInvalidValue", Varint.stringRep))))
    }

    it("when column value not Timeuuid") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "2c530380-b9f9-11e3-850e-338bb2a2e74f"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIME UUID!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlTimeuuid))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TIME UUID!", "hasInvalidValue", CqlTimeuuid.stringRep))))
    }

    it("when column value not List<Int>") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> List(1,2,3,4)),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A LIST!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlList(CqlInt)))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A LIST!", "hasInvalidValue", CqlList(CqlInt).stringRep))))
    }

    it("when column value not Set<Varchar>") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> Set("uno", "dos", "tres")),
            Map("name" -> "listShouldWorkAndTypesShouldBeCoercable", "hasInvalidValue" -> List(1, 2, 3, 4)),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A SET!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlSet(Varchar)))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A SET!", "hasInvalidValue", CqlSet(Varchar).stringRep))))
    }

    it("when column value not Map<Uuid, List<Int>>") {
      // given
      val uuid = UUID.randomUUID().toString
      val mapType = CqlMap(Uuid, CqlList(CqlInt))

      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> Map(uuid -> List(1,2,3,4))),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A MAP!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> mapType))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A MAP!", "hasInvalidValue", mapType.stringRep))))
    }

    it("when column value not tuple (uuid, List<Int>)") {
      // given
      val uuid = UUID.randomUUID().toString
      val tupleType = Tuple(Uuid, CqlList(CqlInt))

      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> (uuid, List(1,2,3,4))),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TUPLE!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> tupleType))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TUPLE!", "hasInvalidValue", tupleType.stringRep))))
    }

    it("when column value not time") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 1368438171000L),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIME!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlTime))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TIME!", "hasInvalidValue", CqlTime.stringRep))))
    }

    it("when column value not date") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 2147484648L),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A DATE!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlDate))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A DATE!", "hasInvalidValue", CqlDate.stringRep))))
    }

    it("when column value not smallint") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 512),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A SMALLINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Smallint))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A SMALLINT!", "hasInvalidValue", Smallint.stringRep))))
    }

    it("when column value not tinyint") {
      // given
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 127),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TINYINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Tinyint))
        )
      )

      // when and then
      val validationResult = new PrimeQueryStore().add(prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TINYINT!", "hasInvalidValue", Tinyint.stringRep))))
    }
  }
}
