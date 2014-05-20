package org.scassandra.priming

import akka.io.IO
import spray.can.Http
import spray.routing._
import spray.util.LoggingContext
import akka.event.Logging
import com.typesafe.scalalogging.slf4j.Logging
import akka.actor.Actor
import org.scassandra.priming.routes.{ActivityVerificationRoute, PrimingQueryRoute, PrimingPreparedRoute}
import org.scassandra.priming.query.PrimeQueryStore
import org.scassandra.priming.prepared.PrimePreparedStore

trait AllRoutes extends HttpService with PrimingPreparedRoute with PrimingQueryRoute with ActivityVerificationRoute with Logging {

  val allRoutes = routeForPreparedPriming ~ queryRoute ~ activityVerificationRoute
}

class PrimingServer(port: Int, implicit val primeQueryStore: PrimeQueryStore, implicit val primePreparedStore : PrimePreparedStore) extends Actor with AllRoutes with Logging {

  implicit def actorRefFactory = context.system

  logger.info(s"Opening port $port for priming")

  IO(Http) ! Http.Bind(self, "localhost", port)

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(allRoutes)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)

  logger.info(s"Server bound to port $port")
}