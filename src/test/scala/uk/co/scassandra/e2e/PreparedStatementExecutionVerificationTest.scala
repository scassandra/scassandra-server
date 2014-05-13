package uk.co.scassandra.e2e

import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import uk.co.scassandra.priming.{PreparedStatementExecution, Query, ActivityLog, PrimingJsonImplicits}
import spray.json._
import uk.co.scassandra.AbstractIntegrationTest
import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import uk.co.scassandra.cqlmessages.ONE

class PreparedStatementExecutionVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  before {
//    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
//    val response = Http(svc OK as.String)
//    response()
  }

  test("Test clearing of prepared statement executions") {
    ActivityLog.clearPreparedStatementExecutions()
    val queryString: String = "select * from people where name = ?"
    val preparedStatement = session.prepare(queryString);
    val boundStatement = preparedStatement.bind("Chris")
    session.execute(boundStatement)
    val svc: Req = url("http://localhost:8043/prepared-statement-execution")
    val delete = svc.DELETE
    val deleteResponse = Http(delete OK as.String)
    deleteResponse()

    val listOfPreparedStatementExecutions = Http(svc OK as.String)
    whenReady(listOfPreparedStatementExecutions) { result =>
      JsonParser(result).convertTo[List[PreparedStatementExecution]].size should equal(0)
    }
  }

  test("Test verification of a single prepared statement executions") {
    ActivityLog.clearPreparedStatementExecutions()
    val queryString: String = "select * from people where name = ?"
    val preparedStatement = session.prepare(queryString);
    val boundStatement = preparedStatement.bind("Chris")
    session.execute(boundStatement)

    val svc: Req = url("http://localhost:8043/prepared-statement-execution")
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      println(result)
      val preparedStatementExecutions = JsonParser(result).convertTo[List[PreparedStatementExecution]]
      preparedStatementExecutions.size should equal(1)
      preparedStatementExecutions(0) should equal(PreparedStatementExecution(queryString, ONE, List()))
    }
  }
}
