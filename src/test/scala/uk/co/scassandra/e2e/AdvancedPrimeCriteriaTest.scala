package uk.co.scassandra.e2e

import uk.co.scassandra.PrimingHelper
import org.scalatest.concurrent.ScalaFutures
import com.datastax.driver.core.{ResultSet, ConsistencyLevel, SimpleStatement}
import dispatch._, Defaults._
import uk.co.scassandra.priming.query.When

class AdvancedPrimeCriteriaTest extends PrimingHelper with ScalaFutures {

  val whenQuery = "select * from people"
  val name = "Chris"
  val nameColumn = "name"
  val rows: List[Map[String, String]] = List(Map(nameColumn -> name))

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Priming by default should apply to the query regardless of consistency") {
    // priming
    prime(When(whenQuery), rows, "success")
  
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.TWO)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ALL)
  }

  test("Priming for a specific consistency should only return results for that consistency") {
    // priming
    prime(When(whenQuery, Some(List("ONE"))), rows, "success")

    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE)
    executeQueryAndVerifyNoResultsConsistencyLevel(ConsistencyLevel.TWO)
  }

  test("Priming for a multiple consistencies should only return results for those consistencies") {
    // priming
    prime(When(whenQuery, Some(List("ONE", "ALL"))), rows, "success")

    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ALL)
    executeQueryAndVerifyNoResultsConsistencyLevel(ConsistencyLevel.TWO)
  }

  test("Priming for same query with different consistencies - success for both") {
    // priming
    val anotherName: String = "anotherName"
    val someDifferentRows: List[Map[String, String]] = List(Map(nameColumn -> anotherName))
    prime(When(whenQuery, Some(List("ONE", "ALL"))), rows, "success")
    prime(When(whenQuery, Some(List("TWO", "THREE"))), someDifferentRows, "success")

    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE, name)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ALL, name)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.TWO, anotherName)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.THREE, anotherName)
  }

  def executeQueryAndVerifyAtConsistencyLevel(consistency: ConsistencyLevel, name : String = name) {
    val statement = new SimpleStatement(whenQuery)
    statement.setConsistencyLevel(consistency)
    val result = session.execute(statement)
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString(nameColumn) should equal(name)
  }

  def executeQueryAndVerifyNoResultsConsistencyLevel(consistency: ConsistencyLevel) {
    val statement = new SimpleStatement(whenQuery)
    statement.setConsistencyLevel(consistency)
    val result = session.execute(statement)
    val results = result.all()
    results.size() should equal(0)
  }
}
