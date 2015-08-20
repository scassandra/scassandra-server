package org.scassandra.server.priming.routes

import org.scassandra.server.priming.batch.{PrimeBatchStore, BatchPrimeSingle}
import org.scassandra.server.priming.json._

import com.typesafe.scalalogging.LazyLogging

import spray.http.StatusCodes
import spray.routing.{Route, HttpService}

trait PrimingBatchRoute extends HttpService with LazyLogging {

  import PrimingJsonImplicits._

  implicit val primeBatchStore: PrimeBatchStore

  val batchRoute: Route = {
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
