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
  implicit val implMetaDat = jsonFormat1(Metadata)
  // let spray know how to convert an incoming JSON request into an instance of PrimeQueryResult
  implicit val impPrimeQueryResult = jsonFormat3(PrimeQueryResult)
  implicit val impConnection = jsonFormat1(Connection)
}

trait PrimingServerRoute extends HttpService with Logging {

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
              val metadata = primeRequest.metadata
              logger.debug(s"Metadata ${metadata}")
              val result = metadata match {
                case Some(metadata) => metadata match {
                  case Metadata(Some("read_request_timeout")) => ReadTimeout
                  case Metadata(Some("unavailable")) => Unavailable
                  case _ => Success
                }
                case None => Success
              }
              primedResults add(primeRequest.when, resultsAsList, result)

              // all good
              StatusCodes.OK
            }
        }
      }
    }
  } ~
  path("connection") {
    get {
      complete {
        ActivityLog.retrieveConnections()
      }
    }
  }
//  } ~
//  path("query") {
//    get {
//      ???
//    }
//  }
}

class PrimingServer(port: Int, implicit val primedResults: PrimedResults) extends Actor with PrimingServerRoute with Logging {

  implicit def actorRefFactory = context.system

  logger.info(s"Opening port ${port} for priming")

  IO(Http) ! Http.Bind(self, "localhost", port)

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(route)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)

  logger.info(s"Server bound to port $port")
}
