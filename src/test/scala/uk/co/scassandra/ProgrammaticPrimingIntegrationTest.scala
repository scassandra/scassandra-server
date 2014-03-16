package uk.co.scassandra

import com.datastax.driver.core.Cluster

class ProgrammaticPrimingIntegrationTest extends AbstractIntegrationTest {
  test("Test prime and query with single row") {
    // priming
    val query = "Test prime and query with single row"
    val rows = List(Map("name"->"Chris"))
    priming().add(query, rows)

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute(query)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")

    cluster.close()
  }
}
