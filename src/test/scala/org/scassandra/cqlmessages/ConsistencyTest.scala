package org.scassandra.cqlmessages

import org.scalatest.{Matchers, FunSuite}

class ConsistencyTest extends FunSuite with Matchers {
  test("Consistency ONE") {
    ONE.code should equal(1)
    ONE.string should equal("ONE")
  }
  test("Consistency TWO") {
    TWO.code should equal(2)
    TWO.string should equal("TWO")
  }
}
