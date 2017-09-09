/*
 * Copyright (C) 2017 Christopher Batey and Dogan Narinc
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

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors.ActivityLogActor._
import org.scassandra.server.priming.json.PrimingJsonImplicits

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//todo make deletes return once actor has confirmed
trait ActivityVerificationRoute extends LazyLogging with SprayJsonSupport {

  import PrimingJsonImplicits._

  implicit val activityLog: ActorRef
  implicit val ec: ExecutionContext
  private implicit val timoeut = Timeout(250 milliseconds)

  val activityVerificationRoute: Route =
    cors() {
      path("connection") {
        get {
          complete {
            (activityLog ? GetAllConnections).mapTo[Connections].map(_.list)
          }
        } ~
          delete {
            complete {
              logger.debug("Deleting all recorded connections")
              activityLog ! ClearConnections
              StatusCodes.OK
            }
          }
      } ~
        path("query") {
          get {
            complete {
              logger.debug("Request for recorded queries")
              (activityLog ? GetAllQueries).mapTo[Queries].map(_.list)
            }
          } ~
            delete {
              complete {
                logger.debug("Deleting all recorded queries")
                activityLog ! ClearQueries
                StatusCodes.OK
              }
            }
        } ~
        path("prepared-statement-preparation") {
          get {
            complete {
              logger.debug("Request for recorded prepared statement preparations")
              (activityLog ? GetAllPrepares).mapTo[Prepares].map(_.list)
            }
          } ~
            delete {
              complete {
                logger.debug("Deleting all recorded prepared statement preparations")
                activityLog ! ClearPrepares
                StatusCodes.OK
              }
            }
        } ~
        path("prepared-statement-execution") {
          get {
            complete {
              logger.debug("Request for recorded prepared statement executions")
              (activityLog ? GetAllExecutions).mapTo[Executions].map(_.list)
            }
          } ~
            delete {
              complete {
                logger.debug("Deleting all recorded prepared statement executions")
                activityLog ! ClearExecutions
                StatusCodes.OK
              }
            }
        } ~
        path("batch-execution") {
          get {
            complete {
              logger.debug("Request for recorded batch executions")
              (activityLog ? GetAllBatches).mapTo[Batches].map(_.list)
            }
          } ~
            delete {
              complete {
                logger.debug("Deleting all recorded batch executions")
                activityLog ! ClearBatches
                StatusCodes.OK
              }
            }
        }
    }
}
