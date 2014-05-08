package uk.co.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import uk.co.scassandra.priming.{PrimingJsonImplicits}
import uk.co.scassandra.priming.prepared.PrimePreparedSingle

trait PrimingPreparedRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  val routeForPreparedPriming =
    path("prime-prepared-single") {
      post {
        entity(as[PrimePreparedSingle]) { prime =>
          complete {
            StatusCodes.OK
          }
        }

      }
    }
}
