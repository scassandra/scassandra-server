/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.pattern.{ask, pipe}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.TcpServer
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.{CompositePreparedPrimeStore, PrimePreparedMultiStore, PrimePreparedPatternStore, PrimePreparedStore}
import org.scassandra.server.priming.query.PrimeQueryStore
import org.scassandra.server.priming.{ActivityLog, AllRoutes}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/**
  * Used to wait on startup of listening http and tcp interfaces.
  *
  * @param timeout Up to how long to wait for startup before timing out.
  */
case class AwaitStartup(timeout: Timeout)

/**
  * Used to shutdown the server by first unbinding the priming and tcp server listeners
  * and then sending a {@link PoisonPill} to itself.
  *
  * @param timeout Up to how long to wait for shutdown before timing out.
  */
case class ShutdownServer(timeout: Timeout)

/**
  * Sent to {@link PrimingServer} and {@link TcpServer} instances to indicate that they should
  * unbind their listeners and then subsequently shutdown.
  */
case object Shutdown

class ScassandraServer(val primeQueryStore: PrimeQueryStore,
                       val binaryListenAddress: String,
                       val binaryPortNumber: Int,
                       val adminListenAddress: String,
                       val adminPortNumber: Int) extends Actor with LazyLogging with AllRoutes {

  val primePreparedStore = new PrimePreparedStore
  val primePreparedPatternStore = new PrimePreparedPatternStore
  val primePreparedMultiStore = new PrimePreparedMultiStore
  val primeBatchStore = new PrimeBatchStore
  val activityLog = new ActivityLog

  val preparedLookup = new CompositePreparedPrimeStore(primePreparedStore, primePreparedPatternStore, primePreparedMultiStore)
  val primingReadyListener: ActorRef = context.actorOf(Props(classOf[ServerReadyListener]), "PrimingReadyListener")
  val tcpReadyListener: ActorRef = context.actorOf(Props(classOf[ServerReadyListener]), "TcpReadyListener")
  val tcpServer: ActorRef = context.actorOf(Props(classOf[TcpServer], binaryListenAddress, binaryPortNumber, primeQueryStore, preparedLookup, primeBatchStore, tcpReadyListener, activityLog), "BinaryTcpListener")

  implicit val dispatcher: ExecutionContext = context.dispatcher
  val timeout = Timeout(5.seconds)

  implicit val materialiser = ActorMaterializer()
  implicit val system = context.system

  val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(allRoutes, adminListenAddress, adminPortNumber)

  override def receive: Receive = {
    case AwaitStartup(timeout) => {
      implicit val t: Timeout = timeout
      // Create a future that completes when both listeners ready.
      val f = Future.sequence(
        tcpReadyListener ? OnServerReady ::
          bindingFuture ::
          Nil
      )

      f pipeTo sender
    }
    case ShutdownServer(timeout) => {
      implicit val t: Timeout = timeout

      // Send shutdown message to each sender and on complete send a PoisonPill to self.
      val f = Future.sequence(
        tcpServer ? Shutdown ::
          bindingFuture.flatMap(_.unbind()) ::
          Nil
      ).map { _ => self ? PoisonPill }

      f pipeTo sender
    }
  }

}
