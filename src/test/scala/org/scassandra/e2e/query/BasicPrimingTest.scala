package org.scassandra.e2e.query

import org.scassandra.AbstractIntegrationTest
import org.scassandra.cqlmessages.{CqlInt, CqlVarchar}
import org.scassandra.priming.query.{Then, When}

class BasicPrimingTest extends AbstractIntegrationTest {
  test("Priming Rows With Different Columns") {
    val query = "select * from people"
    val rowOne = Map("name" -> "Chris")
    val rowTwo = Map("age" -> 15)
    val rows = List(rowOne, rowTwo)
    val columnTypes = Map(
      "name" -> CqlVarchar,
      "age" -> CqlInt)
    prime(When(query), rows, columnTypes = columnTypes)

    val result = session.execute(query)

    val allRows = result.all()
    allRows.size() should equal(2)
    allRows.get(0).getString("name") should equal("Chris")
    allRows.get(1).getInt("age") should equal(15)
  }
}
