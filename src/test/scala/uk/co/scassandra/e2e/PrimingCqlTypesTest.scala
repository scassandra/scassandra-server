package uk.co.scassandra.e2e

import uk.co.scassandra.AbstractIntegrationTest
import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import com.datastax.driver.core.Cluster

class PrimingCqlTypesTest extends AbstractIntegrationTest with ScalaFutures {


  test("Priming with missing type defaults to CqlVarchar") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val svc = url("http://localhost:8043/prime") <<
      s""" {"when":"${whenQuery}", "then": { "rows" :[{"name":"Chris", "age" : "19" }], "column_types" : { "name":"varchar" }}} """  <:<
      Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("19")

    cluster.close()
  }

  test("Priming a CQL int") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val svc = url("http://localhost:8043/prime") <<
      s""" {"when":"${whenQuery}", "then": { "rows" :[{"age":"29"}],
      "column_types" : { "age" : "int" }}} """  <:<
      Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getInt("age") should equal(29)

    cluster.close()
  }
}
