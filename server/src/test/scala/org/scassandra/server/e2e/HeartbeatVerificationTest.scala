package org.scassandra.server.e2e

import com.datastax.driver.core.policies.ConstantReconnectionPolicy
import com.datastax.driver.core.{HostDistance, PoolingOptions, Cluster, SocketOptions}
import org.scassandra.server.{ConnectionToServerStub, AbstractIntegrationTest}

class HeartbeatVerificationTest extends AbstractIntegrationTest(false) {

  test("Should remain connected after heartbeat interval + read timeout.") {
    // Configures a cluster with a heartbeat interval of 1 second, a read timeout
    // of 2 seconds (to provoke quick heartbeat timeouts), and a reconnection
    // timeout of 60 seconds to delay reconnection.
    val cluster = Cluster.builder()
      .addContactPoint(ConnectionToServerStub.ServerHost)
      .withPort(ConnectionToServerStub.ServerPort)
      .withPoolingOptions(new PoolingOptions()
        .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
        .setHeartbeatIntervalSeconds(1))
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(2000))
      .withReconnectionPolicy(new ConstantReconnectionPolicy(60000))
      .build()
    try {
      val session = cluster.connect()

      // Wait for some time after heartbeat interval + read timeout and
      // ensure that we are still connected.
      Thread.sleep(5000)
      session.getState.getConnectedHosts.size() should equal(1)
    } finally {
      cluster.close()
    }
  }
}
