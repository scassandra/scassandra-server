package uk.co.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import uk.co.scassandra.priming.{ActivityLog, PrimingJsonImplicits}

trait ActivityVerificationRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  val activityVerificationRoute =
    path("connection") {
      get {
        complete {
          ActivityLog.retrieveConnections()
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded connections")
            ActivityLog.clearConnections()
            StatusCodes.OK
          }
        }
    } ~
    path("query") {
      get {
        complete {
          logger.debug("Request for recorded queries")
          ActivityLog.retrieveQueries()
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded queries")
            ActivityLog.clearQueries()
            StatusCodes.OK
          }
        }
    }
}
