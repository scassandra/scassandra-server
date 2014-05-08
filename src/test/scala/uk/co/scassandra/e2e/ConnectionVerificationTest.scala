package uk.co.scassandra.e2e

import uk.co.scassandra.{ConnectionToServerStub, PrimingHelper}
import org.scalatest.concurrent.ScalaFutures
import uk.co.scassandra.priming.{PrimingJsonImplicits, Connection, ActivityLog}
import dispatch._, Defaults._
import spray.json.JsonParser
import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions}

class ConnectionVerificationTest extends PrimingHelper with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test verification of connection for a single java driver") {
    ActivityLog.clearConnections()
    val poolingOptions = new PoolingOptions
    poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
    poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 0)
    poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
    poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 0)

    val cluster = Cluster.builder().withPoolingOptions(poolingOptions).addContactPoint(ConnectionToServerStub.ServerHost).withPort(ConnectionToServerStub.ServerPort).build()
    cluster.connect()
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)
    response()

    whenReady(response) {
      result =>
        val connectionList = JsonParser(result).convertTo[List[Connection]]
        // What ever the pooling options are set to the java driver appears to make 2 connections
        // verified with wireshark
        connectionList.size should equal(2)
    }
  }

  test("Test verification of connection when there has been no connections") {
    ActivityLog.clearConnections()
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)

    whenReady(response) {
      result =>
        val connectionList = JsonParser(result).convertTo[List[Connection]]
        connectionList.size should equal(0)
    }
  }
}
