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
package org.scassandra.server

import akka.util.Timeout

import scala.concurrent.duration._

import akka.actor._
import akka.io.{Tcp, IO}

import com.typesafe.scalalogging.LazyLogging

import org.scassandra.server.actors.TcpServer
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.{PreparedMultiStore, CompositePreparedPrimeStore, PrimePreparedPatternStore, PrimePreparedStore}
import org.scassandra.server.priming.query.PrimeQueryStore
import org.scassandra.server.priming.{ActivityLog, PrimingServer}


object ServerStubRunner extends LazyLogging {
  def main(args: Array[String]) {
    val binaryListenAddress = ScassandraConfig.binaryListenAddress
    val binaryPortNumber = ScassandraConfig.binaryPort
    val adminListenAddress = ScassandraConfig.adminListenAddress
    val adminPortNumber = ScassandraConfig.adminPort
    logger.info(s"Using binary port to $binaryPortNumber and admin port to $adminPortNumber")
    val ss = new ServerStubRunner(binaryListenAddress, binaryPortNumber, adminListenAddress, adminPortNumber, ScassandraConfig.startupTimeout)
    ss.start()
    ss.awaitTermination()
  }
}

/**
 * Constructor used by the Java Client so not using any Scala types like Duration.
 */
class ServerStubRunner( val binaryListenAddress: String = "localhost",
                        val binaryPortNumber: Int = 8042,
                        val adminListenAddress: String = "localhost",
                        val adminPortNumber: Int = 8043,
                        val startupTimeoutSeconds: Long = 10) extends LazyLogging {

  var system: ActorSystem = _

  val primedResults = PrimeQueryStore()
  val primePreparedStore = new PrimePreparedStore
  val primePreparedPatternStore = new PrimePreparedPatternStore
  val primePreparedMultiStore = new PreparedMultiStore
  val primeBatchStore = new PrimeBatchStore
  val activityLog = new ActivityLog
  val preparedLookup = new CompositePreparedPrimeStore(primePreparedStore, primePreparedPatternStore, primePreparedMultiStore)

  var primingReadyListener: ActorRef = _
  var tcpReadyListener: ActorRef = _

  def start() = {
    system = ActorSystem(s"CassandraServerStub-$binaryPortNumber-$adminPortNumber")
    val manager = IO(Tcp)(system)
    primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]), "PrimingReadyListener")
    tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]), "TcpReadyListener")
    val tcpServer = system.actorOf(Props(classOf[TcpServer], manager, binaryListenAddress, binaryPortNumber, primedResults, preparedLookup, primeBatchStore, tcpReadyListener, activityLog), "BinaryTcpListener")
    system.actorOf(Props(classOf[PrimingServer], adminListenAddress, adminPortNumber, primedResults, primePreparedStore,
      primePreparedPatternStore, primePreparedMultiStore, primeBatchStore, primingReadyListener, activityLog, tcpServer), "PrimingServer")
  }

  def awaitTermination() = {
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }

  def awaitStartup() = {
    ServerReadyAwaiter.run(primingReadyListener, tcpReadyListener)(Timeout(startupTimeoutSeconds.seconds))
  }
}

