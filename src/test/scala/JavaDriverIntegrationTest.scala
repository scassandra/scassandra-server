import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class JavaDriverIntegrationTest extends FunSuite with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  var serverThread: Thread = null

  test("Should by by default return empty result set for any query") {
    val cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    val session = cluster.connect("people")
    val result = session.execute("select * from pepople")

    result.all().size() should equal(0)

    cluster.shutdown()
  }

  def startServerStub() = {
    serverThread = ServerStubAsThread()
    serverThread.start()
    Thread.sleep(3000)
  }

  def stopServerStub() = {
    ServerStubRunner.shutdown()
    Thread.sleep(3000)
  }

  override def beforeAll() {
    startServerStub()
  }

  override def afterAll() {
    stopServerStub()
  }

}
