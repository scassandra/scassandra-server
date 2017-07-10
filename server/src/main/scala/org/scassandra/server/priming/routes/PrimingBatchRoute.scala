/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.priming.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming.batch.{BatchPrimeSingle, PrimeBatchStore}
import org.scassandra.server.priming.json.PrimingJsonImplicits

trait PrimingBatchRoute extends LazyLogging {

  import PrimingJsonImplicits._

  implicit val primeBatchStore: PrimeBatchStore

  val batchRoute: Route = {
    cors() {
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
        } ~
          delete {
            complete {
              logger.debug("Deleting all batch primes")
              primeBatchStore.clear()
              logger.debug("Return 200")
              StatusCodes.OK
            }
          }
      }
    }
  }
}
