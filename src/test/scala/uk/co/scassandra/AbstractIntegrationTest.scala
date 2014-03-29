package uk.co.scassandra

import java.net.{Socket, ConnectException}
import org.scalatest.{Matchers, BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import uk.co.scassandra.server.ServerStubAsThread
import com.datastax.driver.core.{Session, Cluster}
import uk.co.scassandra.priming.{Then, PrimeQueryResult}
import dispatch._, Defaults._
import spray.json._

abstract class AbstractIntegrationTest extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread : ServerStubAsThread = null

  var cluster : Cluster = _
  var session : Session = _

  import uk.co.scassandra.priming.JsonImplicits._

  def prime(query: String, rows: List[Map[String, String]], result: String, columnTypes: Map[String, String]) = {
    val prime = PrimeQueryResult(query, Then(Some(rows), Some(result), Some(columnTypes))).toJson

    val svc = url("http://localhost:8043/prime") <<
      prime.toString()  <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc OK as.String)
    response()
  }

  def startServerStub() = {
    serverThread = ServerStubAsThread()
    serverThread.start()
    Thread.sleep(3000)
  }

  def stopServerStub() = {
    serverThread.shutdown()
    Thread.sleep(3000)
  }

  def priming() = {
    serverThread.serverStub.primedResults
  }

  override def beforeAll() {
    println("Trying to start server")
    // First ensure nothing else is running on port 8042
    var somethingAlreadyRunning = true

    try {
      ConnectionToServerStub()
      println("Was able to connect to localhost 8042, there must be something running")
    } catch {
      case ce: ConnectException =>
        println("Could not connect to localhost 8042, going to start the server")
        somethingAlreadyRunning = false

    }

    if (somethingAlreadyRunning) {
      fail("There must not be any server already running")
    }

    // Then start the server
    startServerStub()

    cluster = Cluster.builder().addContactPoint("localhost").withPort(8042).build()
    session = cluster.connect("people")
  }

  override def afterAll() {
    stopServerStub()

    cluster.close()
  }
}

object ConnectionToServerStub {
  val ServerHost = "localhost"
  val ServerPort = 8042

  def apply() = {
    val socket = new Socket(ServerHost, ServerPort)
    socket.setSoTimeout(1000)
    socket
  }
}
