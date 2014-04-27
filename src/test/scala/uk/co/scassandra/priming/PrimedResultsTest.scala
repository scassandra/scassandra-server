package uk.co.scassandra.priming

import org.scalatest.{Matchers, FunSpec}
import uk.co.scassandra.cqlmessages._
import java.util.UUID

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
      primeResults add(query, Prime(expectedResult))
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult.get.rows should equal(expectedResult)
    }

    it("should return passed in metadata") {
      // given
      val primeResults = PrimedResults()
      val query: PrimeCriteria = PrimeCriteria("some query", Consistency.all)


      // when
      primeResults add(query, Prime(List(), ReadTimeout))
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
      primeResults add(query, Prime(result))
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
      val result: List[Map[String, String]] = List(Map("name" -> "Mickey", "age" -> "99"))

      // when
      primeResults add(query, Prime(result))
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult.get.rows should equal(result)
    }

    it("should not return if consistency does not match - single") {
      val primeResults = PrimedResults()
      val query = PrimeCriteria("select * from users", List(TWO))
      val result: List[Map[String, String]] = List(Map("name" -> "Mickey", "age" -> "99"))

      // when
      primeResults add(query, Prime(result))
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult should equal(None)
    }

    it("should not return if consistency does not match - many") {
      val primeResults = PrimedResults()
      val query = PrimeCriteria("select * from users", List(TWO, ANY))
      val result: List[Map[String, String]] = List(Map("name" -> "Mickey", "age" -> "99"))

      // when
      primeResults.add(query, Prime(result))
      val actualResult = primeResults.get(PrimeMatch(query.query, ONE))

      // then
      actualResult should equal(None)
    }

    it("should throw something if primes over lap partially") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeForTwoAndAny = PrimeCriteria(query, List(TWO, ANY))
      val primeForThreeAndAny = PrimeCriteria(query, List(THREE, ANY))
      val resultForTwo = Prime(List(Map("name" -> "TWO_ANY")))
      val resultForThree = Prime(List(Map("name" -> "THREE_ANY")))

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
      val resultForTwo = Prime(List(Map("name" -> "FIRST_TIME")))
      val resultForThree = Prime(List(Map("name" -> "SECOND_TIME")))

      primeResults.add(primeForTwoAndAny, resultForTwo)
      primeResults.add(primeForTwoAndAnyAgain, resultForThree)

      val actualResult = primeResults.get(PrimeMatch(query, ANY))
      actualResult.get should equal(resultForThree)
    }

    it("should allow many primes for the same criteria if consistency is different") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeCriteriaForONE = PrimeCriteria(query, List(ONE))
      val primeCriteriaForTWO = PrimeCriteria(query, List(TWO))
      val primeCriteriaForTHREE = PrimeCriteria(query, List(THREE))
      val rowsPrime = Prime(List(Map("name" -> "FIRST_TIME")))

      primeResults.add(primeCriteriaForONE, rowsPrime)
      primeResults.add(primeCriteriaForTWO, rowsPrime)
      primeResults.add(primeCriteriaForTHREE, rowsPrime)
    }
  }

  describe("Get PrimeCriteria by query") {
    it("should get all PrimeCriteria with the same query") {
      val primeResults = PrimedResults()
      val query: String = "select * from users"
      val primeForOneAndTwo = PrimeCriteria(query, List(ONE, TWO))
      val primeForThreeAndAny = PrimeCriteria(query, List(THREE, ANY))
      val resultForTwo = Prime(List(Map("name" -> "FIRST")))
      val resultForThree = Prime(List(Map("name" -> "SECOND")))

      primeResults.add(primeForOneAndTwo, resultForTwo)
      primeResults.add(primeForThreeAndAny, resultForThree)
      val actualResult = primeResults.getPrimeCriteriaByQuery(query)

      actualResult.size should equal(2)
      actualResult(0) should equal(PrimeCriteria(query, List(ONE, TWO)))
      actualResult(1) should equal(PrimeCriteria(query, List(THREE, ANY)))
    }
  }

  // TODO [DN|27-04-2014] - remove ignore flag and implement
  ignore("add() with type mismatch should throw an IllegalArgumentException") {

    def interceptOnAdd(prime: Prime) {
      intercept[IllegalArgumentException] {
        PrimedResults().add(PrimeCriteria("", List()), prime)
      }
    }

    it("when column value not CqlVarchar") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "some varchar"),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlVarchar)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlInt") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "123"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT AN INTEGER!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlInt)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlBoolean") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "true"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT A BOOLEAN!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlBoolean)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlAscii") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "some ascii value"),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlAscii)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlBigint") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "123345"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT A BIGINT!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlBigint)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlCounter") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "1234"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT A COUNTER!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlCounter)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlBlob") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "0x48656c6c6f"),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlBlob)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlDecimal") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlDecimal)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlDouble") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlDouble)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlFloat") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlFloat)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlText") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "undecided"),
          Map("name" -> "catbus", "hasInvalidValue" -> 998) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlText)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlTimestamp") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "1368438171000"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIMESTAMP!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlTimestamp)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlUUID") {
      // given
      val uuid = UUID.randomUUID().toString
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> uuid),
          Map("name" -> "catbus", "hasInvalidValue" -> false) // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlUUID)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlInet") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "127.0.0.1"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT AN INET!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlInet)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlVarint") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "1234"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT A VARINT!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlVarint)
      )

      // when and then
      interceptOnAdd(prime)
    }

    it("when column value not CqlTimeUUID") {
      // given
      val prime = Prime(
        List(
          Map("name" -> "totoro", "hasInvalidValue" -> "2c530380-b9f9-11e3-850e-338bb2a2e74f"),
          Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIME UUID!") // incorrect entry, should trigger exception
        ),
        columnTypes = Map("name" -> CqlVarchar, "hasInvalidValue" -> CqlTimeUUID)
      )

      // when and then
      interceptOnAdd(prime)
    }
  }
}
