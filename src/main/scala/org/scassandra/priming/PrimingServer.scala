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
package org.scassandra.priming

import akka.io.{IO, Tcp}
import spray.can.Http
import spray.routing.{RejectionHandler, ExceptionHandler, RoutingSettings, HttpService}
import spray.util.LoggingContext
import akka.event.Logging
import com.typesafe.scalalogging.slf4j.Logging
import akka.actor.{ActorRef, Props, Actor}
import org.scassandra.priming.routes.{VersionRoute, ActivityVerificationRoute, PrimingQueryRoute, PrimingPreparedRoute}
import org.scassandra.priming.query.PrimeQueryStore
import org.scassandra.priming.prepared.{PrimePreparedPatternStore, PrimePreparedStore}
import org.scassandra.{ServerReady, ScassandraConfig}

trait AllRoutes extends HttpService with PrimingPreparedRoute with PrimingQueryRoute with ActivityVerificationRoute with VersionRoute with Logging {

  val allRoutes = routeForPreparedPriming ~ queryRoute ~ activityVerificationRoute ~ versionRoute
}

class PrimingServer(listenAddress: String, port: Int,
                    implicit val primeQueryStore: PrimeQueryStore,
                    implicit val primePreparedStore: PrimePreparedStore,
                    implicit val primePreparedPatternStore: PrimePreparedPatternStore,
                    serverReadyListener: ActorRef,
                    implicit val activityLog: ActivityLog) extends Actor with Logging {

  import Tcp._

  implicit def actorRefFactory = context.system

  logger.info(s"Opening port ${port} for priming")

  val routing = context.actorOf(Props(classOf[PrimingServerHttpService], primeQueryStore, primePreparedStore, primePreparedPatternStore, activityLog))

  IO(Http) ! Http.Bind(self, listenAddress, port)

  def receive = {
    case Connected(_, _) => {
      sender ! Tcp.Register(routing)
    }
    case b @ Bound(_) => {
      logger.info(s"Priming server bound to admin port $port")
      serverReadyListener ! ServerReady
    }
    case CommandFailed(_) => {
      logger.error(s"Unable to bind to admin port $port. Is it in use?")
      context stop self
      context.system.shutdown()
    }
    case msg @ _ => logger.info(s"Received unknown message $msg")
  }
}

class PrimingServerHttpService(implicit val primeQueryStore: PrimeQueryStore,
                               implicit val primePreparedStore: PrimePreparedStore,
                               implicit val primePreparedPatternStore: PrimePreparedPatternStore,
                               implicit val activityLog: ActivityLog) extends Actor with AllRoutes with Logging {

  implicit def actorRefFactory = context.system

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(allRoutes)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)
}
