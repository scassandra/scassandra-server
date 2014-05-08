package uk.co.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import uk.co.scassandra.priming._
import uk.co.scassandra.priming.PrimeCriteria
import uk.co.scassandra.priming.PrimeQuerySingle
import uk.co.scassandra.priming.Prime

trait PrimingQueryRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val primedResults : PrimedResults

  val queryRoute = {
    path("prime-prepared-sequence") {
      post {
        complete {
          StatusCodes.NotFound
        }
      }
    } ~
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
          println(primedResults)
          val allPrimes: Map[PrimeCriteria, Prime] = primedResults.getAllPrimes()
          PrimeQueryResultExtractor.convertBackToPrimeQueryResult(allPrimes)
        }
      } ~
      post {
        entity(as[PrimeQuerySingle]) {
          primeRequest => {
            complete {
              val primeResult = PrimeQueryResultExtractor.extractPrimeResult(primeRequest)
              val primeCriteria = PrimeQueryResultExtractor.extractPrimeCriteria(primeRequest)
              try {
                primedResults.add(primeCriteria,primeResult)
                StatusCodes.OK
              }
              catch {
                case e: IllegalStateException =>
                  StatusCodes.BadRequest -> new ConflictingPrimes(existingPrimes = primedResults.getPrimeCriteriaByQuery(primeRequest.when.query))
              }
            }
          }
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded priming")
            primedResults.clear()
            logger.debug("Return 200")
            StatusCodes.OK
          }
        }
    }
  }
}
