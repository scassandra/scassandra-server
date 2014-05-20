package org.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import org.scassandra.priming.{PrimingJsonImplicits}
import org.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedStore, PrimePreparedSingle}
import scala.collection.immutable.Iterable
import org.scassandra.priming.query.{TypeMismatches, ConflictingPrimes}

trait PrimingPreparedRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primePreparedStore : PrimePreparedStore

  val routeForPreparedPriming =
    path("prime-prepared-single") {
      post {
        entity(as[PrimePreparedSingle]) { prime =>
          complete {
            primePreparedStore.record(prime) match {
              case cp: ConflictingPrimes => StatusCodes.BadRequest -> cp
              case tm: TypeMismatches => StatusCodes.BadRequest -> tm
              case _ => StatusCodes.OK
            }
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
              WhenPreparedSingle(primeCriteria.query, Some(primeCriteria.consistency)),
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
