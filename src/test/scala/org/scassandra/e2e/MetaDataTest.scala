/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.e2e

import org.scassandra.{ScassandraConfig, PrimingHelper, AbstractIntegrationTest}
import com.datastax.driver.core.Cluster
import org.scassandra.priming.query.When
import org.scassandra.cqlmessages.CqlSet

class MetaDataTest extends AbstractIntegrationTest(false) {


  test("Cluster name") {
    val when = When("SELECT * FROM system.local WHERE key='local'")
    val clusterName = "SomeMetaCluster"
    val columnTypes = Map("tokens" -> CqlSet)
    val rows = List(Map("cluster_name" -> clusterName,
      "partitioner" -> "org.apache.cassandra.dht.Murmur3Partitioner",
      "data_center" -> "dc1",
      "rack" -> "rc1",
      "release_version" -> "2.0.1",
      "tokens" -> Set("1743244960790844724")))
    PrimingHelper.primeQuery(when, rows, columnTypes = columnTypes)

    cluster = Cluster.builder()
      .addContactPoint(ScassandraConfig.binaryListenAddress)
      .withPort(ScassandraConfig.binaryPort)
      .build()
    session = cluster.connect()

    cluster.getMetadata.getClusterName should equal(clusterName)
  }

}
