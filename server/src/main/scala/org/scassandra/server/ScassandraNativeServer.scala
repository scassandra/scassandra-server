/*
 * Copyright (C) 2017 Christopher Batey and Dogan Narinc
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

import akka.actor.{ Actor, ActorRef, ActorSystem, PoisonPill, Props, Scheduler }
import akka.http.scaladsl.Http
import akka.pattern.{ ask, pipe }
import akka.stream.ActorMaterializer
import akka.util.Timeout
import akka.typed.scaladsl.adapter._
import akka.typed.{ ActorRef => TActorRef }
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.ActivityLogTyped.ActivityLogCommand
import org.scassandra.server.actors.priming.{ PreparedPrimesActor, PrimeBatchStoreActor, PrimePreparedStoreActor, PrimeQueryStoreActor }
import org.scassandra.server.actors.{ ActivityLogActor, ActivityLogTyped, TcpServer }
import org.scassandra.server.priming.AllRoutes
import org.scassandra.server.priming.prepared._

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps
import scala.util.{ Failure, Success }
import scala.reflect.runtime.universe._

/**
 * Used to wait on startup of listening http and tcp interfaces.
 *
 * @param timeout Up to how long to wait for startup before timing out.
 */
case class AwaitStartup(timeout: Timeout)

/**
 * Used to shutdown the server by first unbinding the priming and tcp server listeners
 * and then sending a `PoisonPill` to itself.
 *
 * @param timeout Up to how long to wait for shutdown before timing out.
 */
case class ShutdownServer(timeout: Timeout)

/**
 * Sent to `PrimingServer` and `TcpServer` instances to indicate that they should
 * unbind their listeners and then subsequently shutdown.
 */
case object Shutdown

class ScassandraServer(
  val binaryListenAddress: String,
  val binaryPortNumber: Int,
  val adminListenAddress: String,
  val adminPortNumber: Int) extends Actor with LazyLogging with AllRoutes {

  private val legacyPreparedStore = new PrimePreparedStore
  private val legacyPatternStore = new PrimePreparedPatternStore
  private val legacyMultiPSStore = new PrimePreparedMultiStore

  val primePreparedMultiStore = context.actorOf(Props(classOf[PrimePreparedStoreActor[PrimePreparedMulti]], legacyMultiPSStore, typeTag[PrimePreparedMulti]))
  val primePreparedStore = context.actorOf(Props(classOf[PrimePreparedStoreActor[PrimePreparedSingle]], legacyPreparedStore, typeTag[PrimePreparedSingle]))
  val primePreparedPatternStore =
    context.actorOf(Props(classOf[PrimePreparedStoreActor[PrimePreparedSingle]], legacyPatternStore, typeTag[PrimePreparedSingle]))
  private val preparedLookup: ActorRef =
    context.actorOf(Props(classOf[PreparedPrimesActor], List(primePreparedStore, primePreparedPatternStore, primePreparedMultiStore)))

  val activityLogTyped: TActorRef[ActivityLogCommand] = context.spawn(ActivityLogTyped.activityLog, "TypedActivityLog")
  val activityLog: ActorRef = context.actorOf(Props[ActivityLogActor])
  val primeBatchStore: ActorRef = context.actorOf(Props[PrimeBatchStoreActor])
  val primeQueryStore: ActorRef = context.actorOf(Props[PrimeQueryStoreActor])
  val primingReadyListener: ActorRef = context.actorOf(Props(classOf[ServerReadyListener]), "PrimingReadyListener")
  val tcpReadyListener: ActorRef = context.actorOf(Props(classOf[ServerReadyListener]), "TcpReadyListener")
  val tcpServer: ActorRef =
    context.actorOf(Props(classOf[TcpServer], binaryListenAddress, binaryPortNumber, primeQueryStore,
      preparedLookup, primeBatchStore, tcpReadyListener, activityLog), "BinaryTcpListener")

  implicit val ec: ExecutionContext = context.dispatcher
  val scheduler: Scheduler = context.system.scheduler
  val actorTimeout: Timeout = Timeout(2 seconds)

  implicit val system: ActorSystem = context.system
  implicit val materialiser: ActorMaterializer = ActorMaterializer()

  private val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(allRoutes, adminListenAddress, adminPortNumber)

  bindingFuture.onComplete {
    case Success(sb) => logger.info("Successfully bound: {}", sb)
    case Failure(e) => logger.error("Failed to bind for priming http post: ", e)
  }

  override def receive: Receive = {
    case AwaitStartup(startupTimeout) =>
      implicit val t: Timeout = startupTimeout
      // Create a future that completes when both listeners ready.
      val f: Future[List[Any]] = Future.sequence(
        List(ask(tcpReadyListener, OnServerReady)(t), bindingFuture))
      f pipeTo sender
    case ShutdownServer(shutdownTimeout) =>
      implicit val t: Timeout = shutdownTimeout
      // Send shutdown message to each sender and on complete send a PoisonPill to self.
      val f = Future.sequence(
        ask(tcpServer, Shutdown)(t) ::
          bindingFuture.flatMap(_.unbind()) ::
          Nil).map { _ => ask(self, PoisonPill)(t) }
      f pipeTo sender
  }
}
