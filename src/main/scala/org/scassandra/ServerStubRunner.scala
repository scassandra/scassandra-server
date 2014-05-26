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

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.priming.query.PrimeQueryStore
import org.scassandra.priming.prepared.PrimePreparedStore
import org.scassandra.server.TcpServer
import org.scassandra.priming.PrimingServer

object ServerStubRunner extends Logging {
  def main(args: Array[String]) {
    val ss = new ServerStubRunner()
    ss.start()
    ss.awaitTermination()
  }
}

class ServerStubRunner() extends Logging {

  var system: ActorSystem = _

  val primedResults = PrimeQueryStore()
  val primePreparedStore = new PrimePreparedStore

  def start() = {
    system = ActorSystem("CassandraServerStub")
    system.actorOf(Props(classOf[TcpServer], primedResults, primePreparedStore))
    system.actorOf(Props(classOf[PrimingServer], primedResults, primePreparedStore))
  }

  def awaitTermination() = {
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }
}
