package uk.co.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import uk.co.scassandra.priming._
import uk.co.scassandra.priming.query._
import uk.co.scassandra.priming.query.PrimeCriteria
import uk.co.scassandra.priming.query.PrimeQuerySingle
import uk.co.scassandra.priming.query.Prime

trait PrimingQueryRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primeQueryStore: PrimeQueryStore

  val queryRoute = {
      path("prime-query-sequence") {
        post {
          complete {
            // TODO - implement multi primes
            StatusCodes.NotFound
          }
        }
      } ~
      path("prime-query-single") {
        get {
          complete {
            println(primeQueryStore)
            val allPrimes: Map[PrimeCriteria, Prime] = primeQueryStore.getAllPrimes
            PrimeQueryResultExtractor.convertBackToPrimeQueryResult(allPrimes)
          }
        } ~
          post {
            entity(as[PrimeQuerySingle]) {
              primeRequest => {
                complete {
                  val primeResult = PrimeQueryResultExtractor.extractPrimeResult(primeRequest)
                  val primeCriteria = PrimeQueryResultExtractor.extractPrimeCriteria(primeRequest)
                  primeQueryStore.add(primeCriteria, primeResult) match {
                    case cp: ConflictingPrimes => StatusCodes.BadRequest -> cp
                    case tm: TypeMismatches => StatusCodes.BadRequest -> tm
                    case _ => StatusCodes.OK
                  }
                }
              }
            }
          } ~
          delete {
            complete {
              logger.debug("Deleting all recorded priming")
              primeQueryStore.clear()
              logger.debug("Return 200")
              StatusCodes.OK
            }
          }
      }
  }
}
