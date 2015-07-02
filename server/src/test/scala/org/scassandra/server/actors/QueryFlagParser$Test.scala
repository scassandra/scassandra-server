package org.scassandra.server.actors

import org.scalatest.{Matchers, FunSuite}

class QueryFlagParser$Test extends FunSuite with Matchers {
  test("1 should have GlobalTableSpec and not MorePages or NoMetadata") {
    println(QueryFlags.Values)
    QueryFlagParser.hasFlag(QueryFlags.Values, 5) should equal(true)
  }
}
