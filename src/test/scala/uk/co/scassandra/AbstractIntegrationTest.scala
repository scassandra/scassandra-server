package uk.co.scassandra

import java.net.{Socket, ConnectException}
import org.scalatest.{Matchers, BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import uk.co.scassandra.server.ServerStubAsThread
import com.datastax.driver.core.{ConsistencyLevel, Session, Cluster}
import uk.co.scassandra.priming._
import dispatch._, Defaults._
import spray.json._
import scala.Some
import uk.co.scassandra.priming.query.{When, Then, PrimeQuerySingle}
import uk.co.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedSingle}
import uk.co.scassandra.cqlmessages.ColumnType

object PrimingHelper {

  import uk.co.scassandra.priming.PrimingJsonImplicits._

  def primeQuery(query: When, rows: List[Map[String, Any]], result: String = "success", columnTypes: Map[String, ColumnType] = Map()) = {
    val prime = PrimeQuerySingle(query, Then(Some(rows), Some(result), Some(columnTypes))).toJson
    println("Sending JSON: " + prime.toString)
    val svc = url("http://localhost:8043/prime-query-single") <<
      prime.toString() <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc OK as.String)
    response()
  }

  def primePreparedStatement(query: WhenPreparedSingle, then: ThenPreparedSingle) = {
    val prime = PrimePreparedSingle(query, then).toJson
    println("Sending JSON: " + prime.toString)
    val svc = url("http://localhost:8043/prime-prepared-single") <<
      prime.toString <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc OK as.String)
    response()
  }
}

abstract class AbstractIntegrationTest(clusterConnect : Boolean = true) extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread: ServerStubAsThread = null

  var cluster: Cluster = _
  var session: Session = _

  def prime(query: When, rows: List[Map[String, Any]], result: String = "success", columnTypes: Map[String, ColumnType] = Map()) = {
    PrimingHelper.primeQuery(query, rows, result, columnTypes)
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
    // First ensure nothing else is running on the port we are trying to connect to
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

    if (clusterConnect) {
      cluster = Cluster.builder().addContactPoint(ConnectionToServerStub.ServerHost).withPort(ConnectionToServerStub.ServerPort).build()
      session = cluster.connect("mykeyspace")
    }
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
