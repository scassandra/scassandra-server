package org.scassandra.server.priming.prepared

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import org.scassandra.server.cqlmessages.ONE
import org.scassandra.server.cqlmessages.types.CqlText
import org.scassandra.server.priming.{WriteRequestTimeoutResult, ReadRequestTimeoutResult, SuccessResult}
import org.scassandra.server.priming.json.{WriteTimeout, ReadTimeout, Success}
import org.scassandra.server.priming.query.{Prime, PrimeMatch}

import org.scalatest.OptionValues._

// todo genralise all the prepared stores, very little difference
class PreparedMultiStoreTest extends FunSuite with Matchers with BeforeAndAfter {

  var underTest: PreparedMultiStore = _

  before {
    underTest = new PreparedMultiStore
  }

  test("Match on variable type - success") {
    val variableTypes = List(CqlText)
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(VariableMatch(Some("Chris")))), Action(Some(List()), result = Some(Success)))))
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    val preparedPrime = underTest.findPrime(PrimeMatch(queryText, ONE))

    preparedPrime.value.variableTypes should equal(variableTypes)
    preparedPrime.value.getPrime(List(Some("Chris"))) should equal(Prime(rows = List(), result = SuccessResult))
  }

  test("Match on variable type - failure") {
    val variableTypes = List(CqlText)
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(VariableMatch(Some("Chris")))), Action(Some(List()), result = Some(ReadTimeout)))))
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    val preparedPrime = underTest.findPrime(PrimeMatch(queryText, ONE))

    preparedPrime.value.variableTypes should equal(variableTypes)
    preparedPrime.value.getPrime(List(Some("Chris"))) should equal(Prime(result = ReadRequestTimeoutResult()))
  }

  test("Match on variable type - multiple options") {
    val variableTypes = List(CqlText)
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(VariableMatch(Some("Chris")))), Action(Some(List()), result = Some(ReadTimeout))),
      Outcome(Criteria(List(VariableMatch(Some("Daniel")))), Action(Some(List()), result = Some(WriteTimeout)))
    ))
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    val preparedPrime = underTest.findPrime(PrimeMatch(queryText, ONE))

    preparedPrime.value.variableTypes should equal(variableTypes)
    preparedPrime.value.getPrime(List(Some("Daniel"))) should equal(Prime(result = WriteRequestTimeoutResult()))
  }

  // check it matches on consistency
  // return th delay

}
