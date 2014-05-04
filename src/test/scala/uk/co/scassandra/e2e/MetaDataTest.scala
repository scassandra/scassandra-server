package uk.co.scassandra.e2e

import uk.co.scassandra.{ConnectionToServerStub, AbstractIntegrationTest}
import uk.co.scassandra.priming.When
import com.datastax.driver.core.Cluster

class MetaDataTest extends AbstractIntegrationTest(false) {


  ignore("Cluster name") {
    val when = When("SELECT * FROM system.local WHERE key='local'")
    val clusterName = "ACCluster"
    val rows = List(Map("cluster_name" -> clusterName,
      "partitioner" -> "org.apache.cassandra.dht.Murmur3Partitioner",
      "data_center" -> "dc1",
      "rack" -> "rc1",
      "release_version" -> "2.0.1"))
    AbstractIntegrationTest.prime(when, rows)

    cluster = Cluster.builder().addContactPoint(ConnectionToServerStub.ServerHost).withPort(ConnectionToServerStub.ServerPort).build()
    cluster.connect()

    cluster.getClusterName should equal(clusterName)
  }

}
