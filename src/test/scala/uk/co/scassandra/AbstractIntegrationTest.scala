package uk.co.scassandra

import java.net.{Socket, ConnectException}
import org.scalatest.{Matchers, BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import uk.co.scassandra.server.ServerStubAsThread
import com.datastax.driver.core.{Session, Cluster}
import uk.co.scassandra.priming.{When, Then, PrimeQueryResult}
import dispatch._, Defaults._
import spray.json._

abstract class AbstractIntegrationTest extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread : ServerStubAsThread = null

  var cluster : Cluster = _
  var session : Session = _

  import uk.co.scassandra.priming.JsonImplicits._

  def prime(query: When, rows: List[Map[String, String]], result: String = "success", columnTypes: Map[String, String] = Map()) = {
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
      println(s"Succesfully connected to ${ConnectionToServerStub.ServerHost}:${ConnectionToServerStub.ServerPort}. There must be something running.")
    } catch {
      case ce: ConnectException =>
        println(s"No open connection found on ${ConnectionToServerStub.ServerHost}:${ConnectionToServerStub.ServerPort}. Starting the server.")
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
