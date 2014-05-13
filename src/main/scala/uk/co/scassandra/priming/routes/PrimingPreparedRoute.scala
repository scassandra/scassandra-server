package uk.co.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import uk.co.scassandra.priming.{PrimingJsonImplicits}
import uk.co.scassandra.priming.prepared.{PrimePreparedStore, PrimePreparedSingle}

trait PrimingPreparedRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primePreparedStore : PrimePreparedStore

  val routeForPreparedPriming =
    path("prime-prepared-single") {
      post {
        entity(as[PrimePreparedSingle]) { prime =>
          complete {
            logger.info(s"Prepared store $primePreparedStore")
            primePreparedStore.record(prime)
            StatusCodes.OK
          }
        }
      } ~
      delete {
        complete {
          primePreparedStore.clear()
          StatusCodes.OK
        }
      }
    }
}
