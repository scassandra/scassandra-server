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

import akka.actor.{ActorRef, ActorRefFactory, Props, Actor}
import akka.io.{IO, Tcp}
import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress
import org.scassandra.server.priming.ActivityLog
import org.scassandra.server.priming.query.PrimeQueryStore
import org.scassandra.server.priming.prepared.{PreparedStoreLookup, PrimePreparedPatternStore, PrimePreparedStore}
import org.scassandra.server.cqlmessages.CqlMessageFactory
import org.scassandra.server.{ServerReady, ScassandraConfig}

class TcpServer(listenAddress: String, port: Int,
                primedResults: PrimeQueryStore,
                primePrepareStore: PreparedStoreLookup,
                serverReadyListener: ActorRef,
                activityLog: ActivityLog) extends Actor with Logging {

  import Tcp._
  import context.system

  val manager = IO(Tcp)

  IO(Tcp) ! Bind(self, new InetSocketAddress(listenAddress, port))
  val preparedHandler = context.actorOf(Props(classOf[PrepareHandler], primePrepareStore, activityLog))

  def receive = {
    case b @ Bound(localAddress) => {
      logger.info(s"Port $port ready for Cassandra binary connections.")
      serverReadyListener ! ServerReady
    }

    case CommandFailed(_: Bind) => {
      logger.error(s"Unable to bind to port $port for Cassandra binary connections. Is it in use?")
      context stop self
      context.system.shutdown()
    }

    case c @ Connected(remote, local) => {
      logger.debug(s"Incoming connection, creating a connection handler! $remote $local")
      activityLog.recordConnection()
      val handler = context.actorOf(Props(classOf[ConnectionHandler],
        (af: ActorRefFactory, sender: ActorRef, msgFactory: CqlMessageFactory) => af.actorOf(Props(classOf[QueryHandler], sender, primedResults, msgFactory, activityLog)),
        (af: ActorRefFactory, sender: ActorRef, msgFactory: CqlMessageFactory) => af.actorOf(Props(classOf[RegisterHandler], sender, msgFactory)),
        preparedHandler,
        (af: ActorRefFactory, tcpConnection: ActorRef) => af.actorOf(Props(classOf[TcpConnectionWrapper], tcpConnection)))
      )
      logger.debug(s"Sending register with connection handler $handler")
      sender ! Register(handler)
    }
  }
}
