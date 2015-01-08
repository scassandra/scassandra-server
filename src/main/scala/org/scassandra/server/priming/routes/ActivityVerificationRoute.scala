/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
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

import org.scassandra.server.priming.prepared.PrimePreparedStore
import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import spray.http.StatusCodes
import org.scassandra.server.priming.{ActivityLog, PrimingJsonImplicits}

trait ActivityVerificationRoute extends HttpService with Logging {

  import PrimingJsonImplicits._

  implicit val activityLog : ActivityLog

  val activityVerificationRoute =
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
    path("prepared-statement-execution") {
      get {
        complete {
          logger.debug("Request for record prepared statement executions")
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
    }
}
