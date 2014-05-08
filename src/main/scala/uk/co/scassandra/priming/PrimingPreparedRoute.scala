package uk.co.scassandra.priming

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes

trait PrimingPreparedRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  val routeForPreparedPriming =
    path("prime-prepared-single") {
      post {
        complete {
          StatusCodes.NotFound
        }
      }
    }
}
