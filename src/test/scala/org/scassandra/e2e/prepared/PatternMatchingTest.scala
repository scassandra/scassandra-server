package org.scassandra.e2e.prepared

import org.scassandra.{PrimingHelper, AbstractIntegrationTest}
import org.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle}

class PatternMatchingTest extends AbstractIntegrationTest {

  test("Prepared statement should match using a .* without specifying variable types") {
    val preparedStatementText: String = "select * from people where name = ?"
    val preparedStatementRegex: String = "select .* from people .*"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(None, Some(preparedStatementRegex)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }
}
