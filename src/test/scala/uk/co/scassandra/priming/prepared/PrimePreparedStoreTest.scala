package uk.co.scassandra.priming.prepared

import org.scalatest.{Matchers, FunSuite}
import uk.co.scassandra.priming.query.PrimeMatch

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
    actualPrime.get.rows should equal(List())
  }

  test("Store a PreparedPrime and retrieve - many rows") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPreparedSingle(query)
    val rows: List[Map[String, Any]] = List(Map("one"->"two"))
    val then = ThenPreparedSingle(Some(rows))
    val prime = PrimePreparedSingle(when, then)
    //when
    underTest.record(prime)
    val actualPrime = underTest.findPrime(PrimeMatch(query))
    //then
    actualPrime.get.rows should equal(rows)
  }
}
