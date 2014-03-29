package uk.co.scassandra.e2e

import uk.co.scassandra.AbstractIntegrationTest
import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._

class PrimingCqlTypesTest extends AbstractIntegrationTest with ScalaFutures {

  test("Priming with missing type defaults to CqlVarchar") {
    // priming
    val whenQuery = "select * from people"
    val svc = url("http://localhost:8043/prime") <<
      s""" {"when":"${whenQuery}", "then": { "rows" :[{"name":"Chris", "age" : "19" }], "column_types" : { "name":"varchar" }}} """  <:<
      Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("19")

  }

  test("Priming a CQL int") {
    // priming
    val whenQuery = "Test prime and query with a cql int"
    val svc = url("http://localhost:8043/prime") <<
      s""" {"when":"${whenQuery}", "then": { "rows" :[{"age":"29"}],"column_types" : { "age" : "int" }}} """ <:<
      Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getInt("age") should equal(29)

  }

  test("Priming a CQL boolean") {
    // priming
    val whenQuery = "Test prime with cql boolean"
    val svc = url("http://localhost:8043/prime") <<
      s""" {"when":"${whenQuery}", "then": { "rows" :[{"booleanTrue":"true", "booleanFalse":"false"}],
      "column_types" : { "booleanTrue" : "boolean", "booleanFalse" : "boolean" }}} """  <:<
      Map("Content-Type" -> "application/json")
    val response = Http(svc OK as.String)
    response()

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getBool("booleanTrue") should equal(true)
    results.get(0).getBool("booleanFalse") should equal(false)

  }
}
