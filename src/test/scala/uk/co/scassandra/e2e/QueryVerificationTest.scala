package uk.co.scassandra.e2e

import com.datastax.driver.core.Cluster
import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import uk.co.scassandra.priming.{Query, ActivityLog, JsonImplicits}
import spray.json._
import uk.co.scassandra.AbstractIntegrationTest

class QueryVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import JsonImplicits._

  test("Test clearing of query results") {
    ActivityLog.clearQueries()
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
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

    cluster.close()
  }

  test("Test verification of a single query") {
    ActivityLog.clearQueries()
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val queryString: String = "select * from people"
    session.execute(queryString)
    val svc: Req = url("http://localhost:8043/query")
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      val queryList = JsonParser(result).convertTo[List[Query]]
      println(queryList)
      queryList.exists(query => query.query.equals(queryString))
    }

    cluster.close()
  }

}
