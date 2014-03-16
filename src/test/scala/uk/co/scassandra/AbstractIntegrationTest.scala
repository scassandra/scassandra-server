package uk.co.scassandra

import java.net.{Socket, ConnectException}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import uk.co.scassandra.server.ServerStubAsThread

abstract class AbstractIntegrationTest extends FunSuite with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread: Thread = null

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
  }

  override def afterAll() {
    stopServerStub()
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
