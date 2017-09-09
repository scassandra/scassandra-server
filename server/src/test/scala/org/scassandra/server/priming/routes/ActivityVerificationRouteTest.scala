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

import akka.actor.ActorRef
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{ TestActor, TestProbe }
import org.scalatest._
import org.scassandra.codec.Consistency.ONE
import org.scassandra.codec.messages.BatchQueryKind.Simple
import org.scassandra.codec.messages.BatchType._
import org.scassandra.server.actors.ActivityLogActor._
import org.scassandra.server.priming._
import org.scassandra.server.priming.json.PrimingJsonImplicits
import spray.json.JsonParser
import akka.testkit.TestActor._
import org.scassandra.server.actors.Activity._

class ActivityVerificationRouteTest extends FunSpec with BeforeAndAfterEach with Matchers with ScalatestRouteTest with ActivityVerificationRoute {

  implicit def actorRefFactory = system
  val ec = scala.concurrent.ExecutionContext.global
  val activityLogProbe = TestProbe()
  implicit val activityLog = activityLogProbe.ref

  import PrimingJsonImplicits._

  def respondWith(m: Any): Unit = {
    activityLogProbe.setAutoPilot(new AutoPilot {
      override def run(sender: ActorRef, msg: Any): AutoPilot = {
        msg match {
          case _ =>
            sender ! m
            TestActor.NoAutoPilot
        }
      }
    })
  }

  describe("Retrieving connection activity") {
    it("Should return connection count from ActivityLog for single connection") {
      respondWith(Connections(List(Connection())))
      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(1)
        activityLogProbe.expectMsg(GetAllConnections)
      }
    }

    it("Should return connection count from ActivityLog for no connections") {
      respondWith(Connections(List()))
      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(0)
        activityLogProbe.expectMsg(GetAllConnections)
      }
    }

    it("Should clear connections for a delete") {
      Delete("/connection") ~> activityVerificationRoute ~> check {
        activityLogProbe.expectMsg(ClearConnections)
      }
    }
  }

  describe("Retrieving query activity") {

    it("Should return queries from ActivityLog - no queries") {
      respondWith(Queries(List()))
      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(0)
        activityLogProbe.expectMsg(GetAllQueries)
      }
    }

    it("Should return queries from ActivityLog - single query") {

      val queries = List(Query("select 1", ONE, None))
      respondWith(Queries(queries))
      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList should equal(queries)
        activityLogProbe.expectMsg(GetAllQueries)
      }
    }

    it("Should clear queries for a delete") {
      Delete("/query") ~> activityVerificationRoute ~> check {
        activityLogProbe.expectMsg(ClearQueries)
      }
    }
  }

  describe("Primed statement preparations") {
    it("Should return prepared statement preparations from ActivityLog") {
      val ps = PreparedStatementPreparation("cat")
      respondWith(Prepares(List(ps)))

      Get("/prepared-statement-preparation") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementPreparation]]
        response should equal(List(ps))
        activityLogProbe.expectMsg(GetAllPrepares)
      }
    }

    it("Should clear prepared statement preparations for a delete") {
      Delete("/prepared-statement-preparation") ~> activityVerificationRoute ~> check {
        activityLogProbe.expectMsg(ClearPrepares)
      }
    }
  }

  describe("Prepared statement execution") {
    it("Should return prepared statement executions from ActivityLog - single") {
      val pse = PreparedStatementExecution("cat", ONE, None, List(), List(), None)
      respondWith(Executions(List(pse)))
      Get("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementExecution]]
        response should equal(List(pse))
        activityLogProbe.expectMsg(GetAllExecutions)
      }
    }

    it("Should clear prepared statement executions for a delete") {
      Delete("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        activityLogProbe.expectMsg(ClearExecutions)
      }
    }
  }

  describe("Batch execution") {
    it("Should return executions from ActivityLog") {
      val execution = BatchExecution(List(BatchQuery("Query", Simple)), ONE, None, LOGGED, None)
      respondWith(Batches(List(execution)))

      Get("/batch-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[BatchExecution]]
        response should equal(List(execution))
        activityLogProbe.expectMsg(GetAllBatches)
      }
    }

    it("Should clear batch executions for a delete") {
      Delete("/batch-execution") ~> activityVerificationRoute ~> check {
        activityLogProbe.expectMsg(ClearBatches)
      }
    }
  }
}
