package uk.co.scassandra

import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import com.datastax.driver.core.exceptions.{WriteTimeoutException, UnavailableException, ReadTimeoutException}

class JavaDriverIntegrationTest extends AbstractIntegrationTest with ScalaFutures {

  test("Should by by default return empty result set for any query") {
    val result = session.execute("select * from people")

    result.all().size() should equal(0)
  }

  test("Test prime and query with single row") {
    // priming
    val whenQuery = "Test prime and query with single row"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": { "rows" : [{"name":"Chris"}] } }"""  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  test("Test prime and query with many rows") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": { "rows" :[{"name":"Chris"}, {"name":"Alexandra"}] }} """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(1).getString("name") should equal("Alexandra")
  }

  test("Test prime and query with many columns") {
    // priming
    val whenQuery = "Test prime and query with many columns"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": { "rows" :[{"name":"Chris", "age":"28"}, {"name":"Alexandra", "age":"24"}] }} """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("28")
    results.get(1).getString("name") should equal("Alexandra")
    results.get(1).getString("age") should equal("24")
  }

  test("Test read timeout on query") {
    // priming
    val whenQuery = "read timeout query"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": {"result":"read_request_timeout"} } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    intercept[ReadTimeoutException] {
      session.execute(whenQuery)
    }
  }

  test("Test unavailable exception on query") {
    // priming
    val whenQuery = "unavailable exception query"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": {"result":"unavailable"} } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    intercept[UnavailableException] {
      session.execute(whenQuery)
    }
  }

  test("Test write timeout on query") {
    // priming
    val whenQuery = "some write query"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": {"result":"write_request_timeout"} } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    intercept[WriteTimeoutException] {
      session.execute(whenQuery)
    }
  }
}
