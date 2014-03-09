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

}
