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
package org.scassandra

import akka.actor._
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.priming.query.PrimeQueryStore
import org.scassandra.priming.prepared.PrimePreparedStore
import org.scassandra.server.TcpServer
import org.scassandra.priming.PrimingServer

object ServerStubRunner extends Logging {
  def main(args: Array[String]) {
    val binaryPortNumber = ScassandraConfig.binaryPort
    val adminPortNumber = ScassandraConfig.adminPort
    logger.info(s"Using binary port to $binaryPortNumber and admin port to $adminPortNumber")
    val ss = new ServerStubRunner(binaryPortNumber, adminPortNumber)
    ss.start()
    ss.awaitTermination()
  }
}

/**
 * Constructor used by the Java Client so if you change it update the Java Client as well.
 */
class ServerStubRunner(val serverPortNumber: Int = 8042, val adminPortNumber: Int = 8043) extends Logging {

  var system: ActorSystem = _

  val primedResults = PrimeQueryStore()
  val primePreparedStore = new PrimePreparedStore

  var primingReadyListener: ActorRef = _
  var tcpReadyListener: ActorRef = _

  def start() = {
    system = ActorSystem(s"CassandraServerStub-${serverPortNumber}-${adminPortNumber}")
    primingReadyListener = system.actorOf(Props(classOf[ServerReadyListener]), "PrimingReadyListener")
    tcpReadyListener = system.actorOf(Props(classOf[ServerReadyListener]), "TcpReadyListener")
    system.actorOf(Props(classOf[TcpServer], serverPortNumber, primedResults, primePreparedStore, tcpReadyListener), "BinaryTcpListener")
    system.actorOf(Props(classOf[PrimingServer], adminPortNumber, primedResults, primePreparedStore, primingReadyListener), "PrimingServer")
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
    ServerReadyAwaiter.run(primingReadyListener, tcpReadyListener)
  }
}

