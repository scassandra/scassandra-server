package org.scassandra.e2e.prepared

import org.scassandra.cqlmessages.types.{CqlText, CqlInt}
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

  test("Prepared statement should match using a .*  specifying all variable types") {
    val preparedStatementText: String = "select * from people where name = ? and age = ?"
    val preparedStatementRegex: String = "select .* from people .*"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(None, Some(preparedStatementRegex)),
      ThenPreparedSingle(
        Some(List(Map("name" -> "Chris"))),
        Some(List(CqlText, CqlInt))
        )
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris", new Integer(15))

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  test("Prepared statement should match using a .*  specifying a subset variable types") {
    val preparedStatementText: String = "select * from people where age = ? and name = ?"
    val preparedStatementRegex: String = "select .* from people .*"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(None, Some(preparedStatementRegex)),
      ThenPreparedSingle(
        Some(List(Map("name" -> "Chris"))),
        Some(List(CqlInt))
      )
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(new Integer(15), "Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }
}
