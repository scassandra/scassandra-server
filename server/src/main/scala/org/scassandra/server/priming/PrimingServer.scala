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
package org.scassandra.server.priming

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.io.{IO, Tcp}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.ServerReady
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.{PrimePreparedPatternStore, PrimePreparedStore}
import org.scassandra.server.priming.query.PrimeQueryStore
import org.scassandra.server.priming.routes._
import spray.can.Http
import spray.routing.{ExceptionHandler, HttpService, RejectionHandler, RoutingSettings}
import spray.util.LoggingContext

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

trait AllRoutes extends HttpService with PrimingPreparedRoute with
  PrimingQueryRoute with ActivityVerificationRoute with VersionRoute with
  PrimingBatchRoute with CurrentRoute with LazyLogging {

  val allRoutes = routeForPreparedPriming ~
    queryRoute ~ activityVerificationRoute ~
    versionRoute ~ batchRoute ~ currentRoute

}

class PrimingServer(listenAddress: String, port: Int,
                    implicit val primeQueryStore: PrimeQueryStore,
                    implicit val primePreparedStore: PrimePreparedStore,
                    implicit val primePreparedPatternStore: PrimePreparedPatternStore,
                    implicit val primeBatchStore: PrimeBatchStore,
                    serverReadyListener: ActorRef,
                    implicit val activityLog: ActivityLog,
                    tcpServer: ActorRef) extends Actor with LazyLogging {

  import Tcp._

  implicit def actorRefFactory: ActorSystem = context.system

  logger.info(s"Opening port $port for priming")

  val routing = context.actorOf(Props(classOf[PrimingServerHttpService], primeQueryStore, primePreparedStore,
    primePreparedPatternStore, primeBatchStore, activityLog, tcpServer))

  IO(Http) ! Http.Bind(self, listenAddress, port)

  def receive = {
    case Connected(_, _) =>
      sender ! Tcp.Register(routing)
    case b @ Bound(_) =>
      logger.info(s"Priming server bound to admin port $port")
      serverReadyListener ! ServerReady
    case CommandFailed(_) =>
      logger.error(s"Unable to bind to admin port $port. Is it in use?")
      context stop self
      context.system.shutdown()
  }
}

class PrimingServerHttpService(implicit val primeQueryStore: PrimeQueryStore,
                               implicit val primePreparedStore: PrimePreparedStore,
                               implicit val primePreparedPatternStore: PrimePreparedPatternStore,
                               implicit val primeBatchStore: PrimeBatchStore,
                               implicit val activityLog: ActivityLog,
                               override val tcpServer: ActorRef) extends Actor with AllRoutes with LazyLogging {

  implicit def actorRefFactory: ActorSystem = context.system

  implicit val dispatcher: ExecutionContext = context.dispatcher

  val timeout = Timeout(5 seconds)

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(allRoutes)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)
}
