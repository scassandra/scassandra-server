package uk.co.scassandra

import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import com.datastax.driver.core.exceptions.{WriteTimeoutException, UnavailableException, ReadTimeoutException}
import uk.co.scassandra.priming.When

class JavaDriverIntegrationTest extends AbstractIntegrationTest with ScalaFutures {

  test("Should by by default return empty result set for any query") {
    val result = session.execute("select * from people")

    result.all().size() should equal(0)
  }

  test("Test prime and query with single row") {
    // priming
    val whenQuery = "Test prime and query with single row"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris"))
    prime(When(whenQuery), rows)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  test("Test prime and query with many rows") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris"), Map("name"->"Alexandra"))
    prime(When(whenQuery), rows)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(1).getString("name") should equal("Alexandra")
  }

  test("Test prime and query with many columns") {
    // priming
    val whenQuery = "Test prime and query with many columns"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris", "age"->"28"), Map("name"->"Alexandra", "age"->"24"))
    prime(When(whenQuery), rows)

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
    prime(When(whenQuery), List(), "read_request_timeout")

    intercept[ReadTimeoutException] {
      session.execute(whenQuery)
    }
  }

  test("Test unavailable exception on query") {
    // priming
    val whenQuery = "unavailable exception query"
    prime(When(whenQuery), List(), "unavailable")

    intercept[UnavailableException] {
      session.execute(whenQuery)
    }
  }

  test("Test write timeout on query") {
    // priming
    val whenQuery = "some write query"
    prime(When(whenQuery), List(), "write_request_timeout")

    intercept[WriteTimeoutException] {
      session.execute(whenQuery)
    }
  }
}
