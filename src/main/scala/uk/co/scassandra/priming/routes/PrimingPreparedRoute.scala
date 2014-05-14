package uk.co.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import uk.co.scassandra.priming.{PrimingJsonImplicits}
import uk.co.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedStore, PrimePreparedSingle}
import scala.collection.immutable.Iterable

trait PrimingPreparedRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primePreparedStore : PrimePreparedStore

  val routeForPreparedPriming =
    path("prime-prepared-single") {
      post {
        entity(as[PrimePreparedSingle]) { prime =>
          complete {
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
      } ~
      get {
        complete {
          val preparedPrimes: Iterable[PrimePreparedSingle] = primePreparedStore.retrievePrimes().map({case (primeCriteria, preparedPrime) =>
            PrimePreparedSingle(
              WhenPreparedSingle(primeCriteria.query),
              ThenPreparedSingle(
                Some(preparedPrime.prime.rows),
                Some(preparedPrime.variableTypes),
                Some(preparedPrime.prime.columnTypes),
                Some(preparedPrime.prime.result)
              )
            )
          })
          preparedPrimes
        }
      }
    }
}
