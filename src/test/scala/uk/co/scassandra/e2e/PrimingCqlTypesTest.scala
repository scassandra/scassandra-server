package uk.co.scassandra.e2e

import uk.co.scassandra.AbstractIntegrationTest
import org.scalatest.concurrent.ScalaFutures

class PrimingCqlTypesTest extends AbstractIntegrationTest with ScalaFutures {

  test("Priming with missing type defaults to CqlVarchar") {
    // priming
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes: Map[String, String] = Map("name" -> "varchar")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("19")

  }

  test("Priming a CQL int") {
    // priming
    val whenQuery = "Test prime and query with a cql int"
    val rows: List[Map[String, String]] = List(Map("age" -> "29"))
    val columnTypes: Map[String, String] = Map("age" -> "int")
    prime(whenQuery, rows, "success", columnTypes)


    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getInt("age") should equal(29)

  }

  test("Priming a CQL boolean") {
    // priming
    val whenQuery = "Test prime with cql boolean"
    val rows: List[Map[String, String]] = List(Map("booleanTrue" -> "true", "booleanFalse" -> "false"))
    val columnTypes: Map[String, String] = Map("booleanTrue" -> "boolean", "booleanFalse" -> "boolean")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getBool("booleanTrue") should equal(true)
    results.get(0).getBool("booleanFalse") should equal(false)
  }
}
