package org.scassandra.server.priming.routes

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming.batch.{BatchPrimeSingle, PrimeBatchStore}
import org.scassandra.server.priming.cors.CorsSupport
import org.scassandra.server.priming.json.PrimingJsonImplicits
import spray.http.StatusCodes
import spray.routing.{HttpService, Route}

trait PrimingBatchRoute extends HttpService with LazyLogging with CorsSupport {

  import PrimingJsonImplicits._

  implicit val primeBatchStore: PrimeBatchStore

  val batchRoute: Route = {
    cors {
      path("prime-batch-sequence") {
        post {
          complete {
            //todo keep url free for multi primes
            StatusCodes.NotFound
          }
        }
      } ~
      path("prime-batch-single") {
        post {
          entity(as[BatchPrimeSingle]) {
            primeRequest => {
              complete {
                logger.info("Received batch prime {}", primeRequest)
                primeBatchStore.record(primeRequest)
                StatusCodes.OK
              }
            }
          }
        }
      }
    }
  }
}
