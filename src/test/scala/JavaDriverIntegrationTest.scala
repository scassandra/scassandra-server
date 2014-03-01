import com.datastax.driver.core.Cluster

class JavaDriverIntegrationTest extends AbstractIntegrationTest {

  test("Should by by default return empty result set for any query") {
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from pepople")

    result.all().size() should equal(0)

    cluster.close()
  }

}
