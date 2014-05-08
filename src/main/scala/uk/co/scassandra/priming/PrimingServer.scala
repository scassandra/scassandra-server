package uk.co.scassandra.priming

import akka.io.IO
import spray.can.Http
import spray.routing._
import spray.util.LoggingContext
import akka.event.Logging
import com.typesafe.scalalogging.slf4j.Logging
import spray.json._
import spray.httpx.SprayJsonSupport
import spray.http.StatusCodes
import akka.actor.{ActorRefFactory, Actor}
import uk.co.scassandra.cqlmessages._
import uk.co.scassandra.ErrorMessage
import scala.Some

trait PrimingServerRoute extends HttpService with Logging {

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

trait PrimingPrepared extends HttpService with Logging {

  import PrimingJsonImplicits._

  val route =
    path("prime-prepared-single") {
      post {
        complete {
          StatusCodes.NotFound
        }
      }
    }
}

trait ActivityVerification extends HttpService with Logging {

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

trait AllRoutes extends HttpService with PrimingPrepared with PrimingServerRoute with ActivityVerification with Logging {

  val allRoutes = route ~ queryRoute ~ activityVerificationRoute
}

class PrimingServer(port: Int, implicit val primedResults: PrimedResults) extends Actor with AllRoutes with Logging {

  implicit def actorRefFactory = context.system

  logger.info(s"Opening port $port for priming")

  IO(Http) ! Http.Bind(self, "localhost", port)

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(allRoutes)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)

  logger.info(s"Server bound to port $port")
}

case class ConflictingPrimes(existingPrimes: List[PrimeCriteria]) extends ErrorMessage("Conflicting Primes")

case class TypeMismatch(value: String, name: String, columnType: String) extends ErrorMessage("Type mismatch")
