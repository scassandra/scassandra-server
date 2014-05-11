package uk.co.scassandra.priming.prepared

import org.scalatest.{Matchers, FunSuite}
import uk.co.scassandra.priming.query.{Prime, PrimeMatch}
import uk.co.scassandra.cqlmessages.{CqlInet, CqlVarchar}

class PrimePreparedStoreTest extends FunSuite with Matchers {


  test("Store a PreparedPrime and retrieve - empty rows") {
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

  test("Store a PreparedPrime and retrieve - single row without variable type info - defaults to varchar") {
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

  test("Store a PreparedPrime and retrieve - variable type info supplied") {
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

  test("Store a PreparedPrime and retrieve - subset of variable type info supplied") {
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

  test("Store a PreparedPrime and retrieve - single row with all column types") {
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

  test("Store a PreparedPrime and retrieve - subset of column type info supplied (rest defaulted to varchar)") {
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

}
