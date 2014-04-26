package uk.co.scassandra.priming

import org.scalatest.{Matchers, FunSpec}
import uk.co.scassandra.cqlmessages._

class PrimedResultsTest extends FunSpec with Matchers {

  describe("add() and get()") {
    it("should add so that it can be retrieved using get") {
      // given
      val primeResults = PrimedResults()

      val query: PrimeCriteria = PrimeCriteria("select * from users", Consistency.all)
      val expectedResult: List[Map[String, String]] =
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

      // when
      primeResults add (query, expectedResult)
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult.get.rows should equal(expectedResult)
    }

    it("should return passed in metadata") {
      // given
      val primeResults = PrimedResults()
      val query: PrimeCriteria = PrimeCriteria("some query", Consistency.all)


      // when
      primeResults add (query, List(), ReadTimeout)
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult.get.result should equal(ReadTimeout)
    }
  }

  describe("get()") {
    it("should return Nil if no results for given query") {
      // given
      val primeResults = PrimedResults()
      // when
      val actualResult = primeResults.get(PrimeMatch("some random query", ONE))

      // then
      actualResult.isEmpty should equal(true)
    }
  }

  describe("clear()") {
    it("should clear all results") {
      // given
      val primeResults = PrimedResults()
      val query: PrimeCriteria = PrimeCriteria("select * from users", Consistency.all)
      val result: List[Map[String, String]] =
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

      // when
      primeResults add (query, result)
      primeResults clear()
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult.isEmpty should equal(true)
    }
  }

  describe("add() with specific consistency") {
    it("should only return if consistency matches - single") {
      val primeResults = PrimedResults()
      val query = PrimeCriteria("select * from users", List(ONE))
      val result: List[Map[String, String]] = List( Map("name" -> "Mickey","age" -> "99"))

      // when
      primeResults add (query, result)
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult.get.rows should equal(result)
    }

    it("should not return if consistency does not match - single") {
      val primeResults = PrimedResults()
      val query = PrimeCriteria("select * from users", List(TWO))
      val result: List[Map[String, String]] = List( Map("name" -> "Mickey","age" -> "99"))

      // when
      primeResults add (query, result)
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult should equal(None)
    }

    it("should not return if consistency does not match - many") {
      val primeResults = PrimedResults()
      val query = PrimeCriteria("select * from users", List(TWO, ANY))
      val result: List[Map[String, String]] = List( Map("name" -> "Mickey","age" -> "99"))

      // when
      primeResults.add(query, result)
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult should equal(None)
    }

    it("should throw something if primes over lap partially") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeForTwoAndAny = PrimeCriteria(query, List(TWO, ANY))
      val primeForThreeAndAny = PrimeCriteria(query, List(THREE, ANY))
      val resultForTwo: List[Map[String, String]] = List( Map("name" -> "TWO_ANY"))
      val resultForThree: List[Map[String, String]] = List( Map("name" -> "THREE_ANY"))

      intercept[IllegalStateException] {
        primeResults.add(primeForTwoAndAny, resultForTwo)
        primeResults.add(primeForThreeAndAny, resultForThree)
      }
    }

    it("should override if it is the same prime criteria") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeForTwoAndAny = PrimeCriteria(query, List(TWO, ANY))
      val primeForTwoAndAnyAgain = PrimeCriteria(query, List(TWO, ANY))
      val resultForTwo: List[Map[String, String]] = List( Map("name" -> "FIRST_TIME"))
      val resultForThree: List[Map[String, String]] = List( Map("name" -> "SECOND_TIME"))

      primeResults.add(primeForTwoAndAny, resultForTwo)
      primeResults.add(primeForTwoAndAnyAgain, resultForThree)

      val actualResult = primeResults.get(PrimeMatch(query, ANY))
      actualResult.get.rows should equal(resultForThree)
    }
    
    it("should allow many primes for the same criteria if consistency is different") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeForONE = PrimeCriteria(query, List(ONE))
      val primeForTWO = PrimeCriteria(query, List(TWO))
      val primeForTHREE = PrimeCriteria(query, List(THREE))
      val rows: List[Map[String, String]] = List( Map("name" -> "FIRST_TIME"))

      primeResults.add(primeForONE, rows)
      primeResults.add(primeForTWO, rows)
      primeResults.add(primeForTHREE, rows)
    }

  }

  describe("Get PrimeCriteria by query") {
    it("should get all PrimeCriteria with the same query") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeForOneAndTwo = PrimeCriteria(query, List(ONE, TWO))
      val primeForThreeAndAny = PrimeCriteria(query, List(THREE, ANY))
      val resultForTwo: List[Map[String, String]] = List( Map("name" -> "FIRST"))
      val resultForThree: List[Map[String, String]] = List( Map("name" -> "SECOND"))

      primeResults.add(primeForOneAndTwo, resultForTwo)
      primeResults.add(primeForThreeAndAny, resultForThree)
      val actualResult = primeResults.getPrimeCriteriaByQuery(query)

      actualResult.size should equal(2)
      actualResult(0) should equal(PrimeCriteria(query, List(ONE, TWO)))
      actualResult(1) should equal(PrimeCriteria(query, List(THREE, ANY)))
    }
  }


}
