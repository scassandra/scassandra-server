package uk.co.scassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.exceptions.ReadTimeoutException
import uk.co.scassandra.priming.{Success, ReadTimeout}
import com.batey.narinc.client.cqlmessages.{ColumnType, CqlVarchar}

class ProgrammaticPrimingIntegrationTest extends AbstractIntegrationTest {
  test("Test prime and query with single row") {
    // priming
    val query = "Test prime and query with single row"
    val rows = List(Map("name"->"Chris"))
    val types = Map[String, ColumnType]("name"->CqlVarchar)
    priming().add(query, rows, Success, types)

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(query)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")

    cluster.close()
  }

  test("Test read timeout on query") {
    // priming
    val query = "Test prime and query with single row"
    priming().add(query, List(), ReadTimeout)

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")

    intercept[ReadTimeoutException] {
      session.execute(query)
    }

    cluster.close()
  }
}
