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

import org.scassandra.{ConnectionToServerStub, AbstractIntegrationTest}
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.priming.{PrimingJsonImplicits, Connection, ActivityLog}
import dispatch._, Defaults._
import spray.json.JsonParser
import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions}

class ConnectionVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test verification of connection for a single java driver") {
    ActivityLog.clearConnections()

    val cluster = Cluster.builder()
      .addContactPoint(ConnectionToServerStub.ServerHost)
      .withPort(ConnectionToServerStub.ServerPort)
      .build()
    cluster.connect()
    val svc: Req = url("http://localhost:8043/connection")
    val response = Http(svc OK as.String)
    response()

    whenReady(response) {
      result =>
        val connectionList = JsonParser(result).convertTo[List[Connection]]
        // What ever the pooling options are set to the java driver appears to make 3 connections
        // verified with wireshark
        connectionList.size should equal(3)
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
