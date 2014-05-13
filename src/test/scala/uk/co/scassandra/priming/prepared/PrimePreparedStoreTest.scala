package uk.co.scassandra.priming.prepared

import org.scalatest.{Matchers, FunSuite}
import uk.co.scassandra.priming.query.{Prime, PrimeMatch}
import uk.co.scassandra.cqlmessages.{CqlInet, CqlVarchar}
import uk.co.scassandra.priming.ReadTimeout

class PrimePreparedStoreTest extends FunSuite with Matchers {


  test("Empty rows") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPreparedSingle(query)
    val then = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, then)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get.prime.rows should equal(List())
  }

  test("Single row without variable type info - defaults to varchar") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPreparedSingle(query)
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val then = ThenPreparedSingle(Some(rows))
    val prime = PrimePreparedSingle(when, then)
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
    val when = WhenPreparedSingle(query)
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val variableTypes = List(CqlInet, CqlInet)
    val then = ThenPreparedSingle(Some(rows), Some(variableTypes))
    val prime = PrimePreparedSingle(when, then)
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
    val when = WhenPreparedSingle(query)
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val variableTypes = List(CqlInet)
    val then = ThenPreparedSingle(Some(rows), Some(variableTypes))
    val prime = PrimePreparedSingle(when, then)
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
    val when = WhenPreparedSingle(query)
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val columnTypes = Map("one"->CqlInet)
    val then = ThenPreparedSingle(Some(rows), column_types = Some(columnTypes))
    val prime = PrimePreparedSingle(when, then)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(List(CqlVarchar, CqlVarchar), prime = Prime(rows, columnTypes = columnTypes)))
  }

  test("Subset of column type info supplied - rest defaulted to varchar)") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ? and age = ?"
    val when = WhenPreparedSingle(query)
    val rows: List[Map[String, Any]] = List(Map("column_type_specified" -> "two","column_type_not_specified"->"three"))
    val columnTypes = Map("column_type_specified" -> CqlInet)
    val then = ThenPreparedSingle(Some(rows), column_types = Some(columnTypes))
    val prime = PrimePreparedSingle(when, then)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    val expectedColumnTypes = Map("column_type_specified" -> CqlInet, "column_type_not_specified" -> CqlVarchar)
    actualPrime.get should equal(PreparedPrime(List(CqlVarchar, CqlVarchar), prime = Prime(rows, columnTypes = expectedColumnTypes)))
  }

  test("Specifying result other than success") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people"
    val when = WhenPreparedSingle(query)
    val then = ThenPreparedSingle(None, result = Some(ReadTimeout))
    val prime = PrimePreparedSingle(when, then)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get should equal(PreparedPrime(List(), Prime(result = ReadTimeout)))
  }

  test("Clearing all the primes") {
    //given
    val underTest = new PrimePreparedStore
    val when = WhenPreparedSingle("")
    val then = ThenPreparedSingle(None)
    val prime = PrimePreparedSingle(when, then)
    underTest.record(prime)
    //when
    underTest.clear()
    //then
    underTest.state.size should equal(0)
  }
}
