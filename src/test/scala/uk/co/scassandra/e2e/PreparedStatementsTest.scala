package uk.co.scassandra.e2e

import uk.co.scassandra.{PrimingHelper, AbstractIntegrationTest}
import uk.co.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle}

class PreparedStatementsTest extends AbstractIntegrationTest {
  test("Prepared statement without priming - no params") {
    //given
    val preparedStatement = session.prepare("select * from people")
    val boundStatement = preparedStatement.bind()

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement without priming - single params") {
    //given
    val preparedStatement = session.prepare("select * from people where name = ?")
    val boundStatement = preparedStatement.bind("name")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement for schema change") {
    //given
    val preparedStatement = session.prepare("CREATE KEYSPACE ? WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ?, 'dc2': ?};")
    val boundStatement = preparedStatement.bind("keyspaceName","3","1")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - empty rows") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(List()))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - single row") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
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
