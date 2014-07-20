package org.scassandra.e2e.query

import org.scassandra.AbstractIntegrationTest
import org.scassandra.priming.query.When

class PatternMatchingTest extends AbstractIntegrationTest {

  test("Query should match using a *") {
    val queryWithStar = "insert into blah with ttl = .*"
    val rowOne = Map("name" -> "Chris")
    prime(When(queryPattern = Some(queryWithStar)), List(rowOne))

    val result = session.execute("insert into blah with ttl = 1234")

    val allRows = result.all()
    allRows.size() should equal(1)
    allRows.get(0).getString("name") should equal("Chris")
  }
}
