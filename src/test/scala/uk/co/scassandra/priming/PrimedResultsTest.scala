package uk.co.scassandra.priming

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class PrimedResultsTest extends FunSpec with ShouldMatchers {

  describe("add() and get()") {
    it("should add so that it can be retrieved using get") {
      // given
      val primeResults = PrimedResults()

      val query: String = "select * from users"
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
      val actualResult = primeResults get query

      // then
      actualResult.get.rows should equal(expectedResult)
    }

    it("should return passed in metadata") {
      // given
      val primeResults = PrimedResults()
      val query = "some query"

      // when
      primeResults add (query, List(), ReadTimeout)
      val actualResult = primeResults get query

      // then
      actualResult.get.result should equal(ReadTimeout)
    }
  }

  describe("get()") {
    it("should return Nil if no results for given query") {
      // given
      val primeResults = PrimedResults()
      // when
      val actualResult = primeResults get "some random query"

      // then
      actualResult.isEmpty should equal(true)
    }
  }

  describe("clear()") {
    it("should clear all results") {
      // given
      val primeResults = PrimedResults()

      val query: String = "select * from users"
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
      val actualResult = primeResults get query

      // then
      actualResult.isEmpty should equal(true)
    }
  }
}
