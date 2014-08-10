package org.scassandra.priming.prepared

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import org.scassandra.cqlmessages._
import org.scassandra.cqlmessages.types.{CqlInt, CqlInet, CqlVarchar}
import org.scassandra.priming.Success
import org.scassandra.priming.query.{Prime, PrimeMatch}

class PrimePreparedPatternStoreTest extends FunSuite with Matchers with BeforeAndAfter {

  var underTest : PrimePreparedPatternStore= _

  before {
    underTest = new PrimePreparedPatternStore
  }

  test("Should return None if pattern does not match") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), Some(List(ONE)))
    val then = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, then)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from users where age = '6'"))
    result.isDefined should equal(false)
  }

  test("Should find prime if pattern and consistency match") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern), consistency = Some(List(ONE)))
    val then = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, then)

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
    val then = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, then)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = '6'", THREE))
    result.isDefined should equal(false)
  }


  test("Defaults consistencies to all") {
    val pattern = "select .* from people.*"
    val when = WhenPreparedSingle(None, Some(pattern))
    val then = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, then)

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
    val then = ThenPreparedSingle(Some(rows))

    val preparedPrime = PrimePreparedSingle(when, then)

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
    val then = ThenPreparedSingle(Some(rows))

    val preparedPrime = PrimePreparedSingle(when, then)

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
    val then = ThenPreparedSingle(Some(rows), Some(List(CqlInet)))

    val preparedPrime = PrimePreparedSingle(when, then)

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
    val then = ThenPreparedSingle(Some(rows), Some(List(CqlInet, CqlInt)))

    val preparedPrime = PrimePreparedSingle(when, then)

    //when
    underTest.record(preparedPrime)

    //then
    val result: Option[PreparedPrime] = underTest.findPrime(PrimeMatch("select name from people where age = ? and name = ?", ONE))
    result.get.variableTypes should equal(List(CqlInet, CqlInt))
  }

  //todo validation
  //clearing

}
