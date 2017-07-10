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

import org.scassandra.server.priming.ActivityLog
import org.scassandra.server.priming.json.PrimingJsonImplicits

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging

trait ActivityVerificationRoute extends LazyLogging {

  import PrimingJsonImplicits._

  implicit val activityLog: ActivityLog

  val activityVerificationRoute: Route =
    cors() {
      path("connection") {
        get {
          complete {
            activityLog.retrieveConnections()
          }
        } ~
        delete {
          complete {
            logger.debug("Deleting all recorded connections")
            activityLog.clearConnections()
            StatusCodes.OK
          }
        }
      } ~
      path("query") {
        get {
          complete {
            logger.debug("Request for recorded queries")
            activityLog.retrieveQueries()
          }
        } ~
        delete {
          complete {
            logger.debug("Deleting all recorded queries")
            activityLog.clearQueries()
            StatusCodes.OK
          }
        }
      } ~
      path("prepared-statement-preparation") {
        get {
          complete {
            logger.debug("Request for recorded prepared statement preparations")
            activityLog.retrievePreparedStatementPreparations()
          }
        } ~
        delete {
          complete {
            logger.debug("Deleting all recorded prepared statement preparations")
            activityLog.clearPreparedStatementPreparations()
            StatusCodes.OK
          }
        }
      } ~
      path("prepared-statement-execution") {
        get {
          complete {
            logger.debug("Request for recorded prepared statement executions")
            activityLog.retrievePreparedStatementExecutions()
          }
        } ~
        delete {
          complete {
            logger.debug("Deleting all recorded prepared statement executions")
            activityLog.clearPreparedStatementExecutions()
            StatusCodes.OK
          }
        }
      } ~
      path("batch-execution") {
        get {
          complete {
            logger.debug("Request for recorded batch executions")
            activityLog.retrieveBatchExecutions()
          }
        } ~
        delete {
          complete {
            logger.debug("Deleting all recorded batch executions")
            activityLog.clearBatchExecutions()
            StatusCodes.OK
          }
        }
      }
    }
}
