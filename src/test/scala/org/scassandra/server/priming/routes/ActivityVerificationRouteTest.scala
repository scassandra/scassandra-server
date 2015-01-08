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

import org.scalatest.{Matchers, BeforeAndAfter, FunSpec}
import spray.testkit.ScalatestRouteTest
import org.scassandra.server.priming._
import spray.json.JsonParser
import org.scassandra.server.cqlmessages.ONE
import org.scassandra.server.priming.Connection
import org.scassandra.server.priming.Query

class ActivityVerificationRouteTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with ActivityVerificationRoute {

  implicit def actorRefFactory = system
  implicit val activityLog = new ActivityLog

  import PrimingJsonImplicits._

  before {
    activityLog.clearConnections()
    activityLog.clearPreparedStatementExecutions()
    activityLog.clearQueries()
  }

  describe("Retrieving connection activity") {
    it("Should return connection count from ActivityLog for single connection") {
      activityLog.recordConnection()

      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(1)
      }
    }

    it("Should return connection count from ActivityLog for no connections") {

      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(0)
      }
    }

    it("Should clear connections for a delete") {
      //todo clear activity

      Delete("/connection") ~> activityVerificationRoute ~> check {
        activityLog.retrieveConnections().size should equal(0)
      }
    }
  }

  describe("Retrieving query activity") {

    it("Should return queries from ActivityLog - no queries") {

      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - single query") {
      val query: String = "select * from people"
      activityLog.recordQuery(query, ONE)

      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(1)
        queryList(0).query should equal(query)
      }
    }

    it("Should clear queries for a delete") {
      activityLog.recordQuery("select * from people", ONE)

      Delete("/query") ~> activityVerificationRoute ~> check {
        activityLog.retrieveQueries().size should equal(0)
      }
    }
  }

  describe("Primed statement execution") {
    it("Should return prepared statement executions from ActivityLog - no activity") {
      activityLog.clearPreparedStatementExecutions()

      Get("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementExecution]]
        response.size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - single query") {
      activityLog.clearPreparedStatementExecutions()
      val preparedStatementText: String = ""
      activityLog.recordPreparedStatementExecution(preparedStatementText, ONE, List())

      Get("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementExecution]]

        response.size should equal(1)
        response(0).preparedStatementText should equal(preparedStatementText)
      }
    }

    it("Should clear queries for a delete") {
      activityLog.recordPreparedStatementExecution("", ONE, List())

      Delete("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        activityLog.retrievePreparedStatementExecutions().size should equal(0)
      }
    }
  }

}
