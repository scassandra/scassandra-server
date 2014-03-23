package uk.co.scassandra

import com.datastax.driver.core.{HostDistance, PoolingOptions, Cluster}
import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import com.datastax.driver.core.exceptions.{WriteTimeoutException, UnavailableException, ReadTimeoutException}
import uk.co.scassandra.priming.{Query, ActivityLog, JsonImplicits, Connection}
import spray.json._

class JavaDriverIntegrationTest extends AbstractIntegrationTest with ScalaFutures {

  import JsonImplicits._

  test("Should by by default return empty result set for any query") {
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from people")

    result.all().size() should equal(0)

    cluster.close()
  }

  test("Test prime and query with single row") {
    // priming
    val whenQuery = "Test prime and query with single row"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": { "rows" : [{"name":"Chris"}] } }"""  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")

    cluster.close()
  }

  test("Test prime and query with many rows") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": { "rows" :[{"name":"Chris"}, {"name":"Alexandra"}] }} """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(1).getString("name") should equal("Alexandra")

    cluster.close()
  }

  test("Test prime and query with many columns") {
    // priming
    val whenQuery = "Test prime and query with many columns"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": { "rows" :[{"name":"Chris", "age":"28"}, {"name":"Alexandra", "age":"24"}] }} """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("28")
    results.get(1).getString("name") should equal("Alexandra")
    results.get(1).getString("age") should equal("24")

    cluster.close()
  }

  test("Test read timeout on query") {
    // priming
    val whenQuery = "read timeout query"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": {"result":"read_request_timeout"} } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")

    intercept[ReadTimeoutException] {
      session.execute(whenQuery)
    }

    cluster.close()
  }

  test("Test unavailable exception on query") {
    // priming
    val whenQuery = "unavailable exception query"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": {"result":"unavailable"} } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")

    intercept[UnavailableException] {
      session.execute(whenQuery)
    }

    cluster.close()
  }

  test("Test write timeout on query") {
    // priming
    val whenQuery = "some write query"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": {"result":"write_request_timeout"} } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")

    intercept[WriteTimeoutException] {
      session.execute(whenQuery)
    }

    cluster.close()
  }

  test("Test verification of connection when there has been no connections") {
    ActivityLog.clearConnections()
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      val connectionList = JsonParser(result).convertTo[List[Connection]]
      connectionList.size should equal(0)
    }
  }

  test("Test verification of connection for a single java driver") {
    ActivityLog.clearConnections()
    val poolingOptions = new PoolingOptions
    poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
    poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 0)
    poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
    poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 0)
    val cluster = Cluster.builder().withPoolingOptions(poolingOptions).addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      val connectionList = JsonParser(result).convertTo[List[Connection]]
      // What ever the pooling options are set to the java driver appears to make 2 connections
      // verified with wireshark
      connectionList.size should equal(2)
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
}
