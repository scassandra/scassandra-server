package uk.co.scassandra.e2e

import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import uk.co.scassandra.priming.{Query, ActivityLog, PrimingJsonImplicits}
import spray.json._
import uk.co.scassandra.AbstractIntegrationTest
import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import uk.co.scassandra.cqlmessages.TWO

class QueryVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test clearing of query results") {
    ActivityLog.clearQueries()
    val queryString: String = "select * from people"
    session.execute(queryString)
    val svc: Req = url("http://localhost:8043/query")
    val delete = svc.DELETE
    val deleteResponse = Http(delete OK as.String)
    deleteResponse()

    val listOfQueriesResponse = Http(svc OK as.String)
    whenReady(listOfQueriesResponse) { result =>
      JsonParser(result).convertTo[List[Query]].size should equal(0)
    }
  }

  test("Test verification of a single query") {
    ActivityLog.clearQueries()
    val queryString: String = "select * from people"
    val statement = new SimpleStatement(queryString)
    statement.setConsistencyLevel(ConsistencyLevel.TWO)
    session.execute(queryString)
    val svc: Req = url("http://localhost:8043/query")
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      val queryList = JsonParser(result).convertTo[List[Query]]
      println(queryList)
      queryList.exists(_ == Query(queryString, TWO))
    }
  }
}
