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

import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.priming.PrimeBatchStoreActor.{BatchPrimeSingle, ClearPrimes, RecordBatchPrime}
import org.scassandra.server.priming.json.PrimingJsonImplicits
import scala.concurrent.duration._

import scala.util.Success

trait PrimingBatchRoute extends LazyLogging {

  import PrimingJsonImplicits._

  val primeBatchStore: ActorRef
  implicit private val timeout: Timeout = Timeout(1 second)

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
                  primeBatchStore ! RecordBatchPrime(primeRequest)
                  StatusCodes.OK
                }
              }
            }
          } ~
            delete {
              logger.debug("Deleting all batch primes")
              onComplete(primeBatchStore ? ClearPrimes) {
                case Success(Done) => complete(StatusCodes.OK)
              }
            }
        }
    }
  }
}
