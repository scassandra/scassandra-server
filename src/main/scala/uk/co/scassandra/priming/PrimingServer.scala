package uk.co.scassandra.priming

import akka.io.IO
import spray.can.Http
import spray.routing._
import spray.util.LoggingContext
import akka.event.Logging
import com.typesafe.scalalogging.slf4j.Logging
import spray.json.DefaultJsonProtocol
import spray.httpx.SprayJsonSupport
import spray.http.StatusCodes
import akka.actor.Actor

object JsonImplicits extends DefaultJsonProtocol with SprayJsonSupport {
  // let spray know how to convert an incoming JSON request into an instance of PrimeQueryResult
  implicit val impPrimeQueryResult = jsonFormat2(PrimeQueryResult)
}

trait PrimingServerRoute extends HttpService {

  import JsonImplicits._

  implicit val primedResults: PrimedResults

  val route = {
    path("prime") {
      post {
        entity(as[PrimeQueryResult]) {
          primeRequest =>
            complete {
              // add the deserialized JSON request to the map of prime requests
              val resultsAsList = primeRequest.then.convertTo[List[Map[String, String]]]
              primedResults add (primeRequest.when -> resultsAsList)

              // all good
              StatusCodes.OK
            }
        }
      }
    }
  }
}

class PrimingServer(port: Int, implicit val primedResults: PrimedResults) extends Actor with PrimingServerRoute with Logging {

  implicit def actorRefFactory = context.system

  IO(Http) ! Http.Bind(self, "localhost", port)

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(route)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)

  logger.info(s"Server bound to port $port")
}
