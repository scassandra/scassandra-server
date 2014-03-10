package uk.co.scassandra.server

import com.datastax.driver.core.Cluster
import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._

class JavaDriverIntegrationTest extends AbstractIntegrationTest with ScalaFutures {

  test("Should by by default return empty result set for any query") {
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from people")

    result.all().size() should equal(0)

    cluster.close()
  }

  test("Test prime and query with single row") {
    // priming
    val whenQuery = "select * from people"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": [{"name":"Chris"}] } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from people")

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")

    cluster.close()
  }

  test("Test prime and query with many rows") {
    // priming
    val whenQuery = "select * from people"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": [{"name":"Chris"}, {"name":"Alexandra"}] } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from people")

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(1).getString("name") should equal("Alexandra")

    cluster.close()
  }

  test("Test prime and query with many columns") {
    // priming
    val whenQuery = "select * from people"
    val svc = url("http://localhost:8043/prime") << s""" {"when":"${whenQuery}", "then": [{"name":"Chris", "age":"28"}, {"name":"Alexandra", "age":"24"}] } """  <:< Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from people")

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("28")
    results.get(1).getString("name") should equal("Alexandra")
    results.get(1).getString("age") should equal("24")

    cluster.close()
  }

}
