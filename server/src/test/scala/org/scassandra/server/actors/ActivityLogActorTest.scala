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
package org.scassandra.server.actors

import akka.actor.Props
import akka.testkit.ImplicitSender
import org.scalatest.{ Matchers, WordSpec }
import org.scassandra.codec.Consistency
import org.scassandra.codec.messages.BatchType
import org.scassandra.server.actors.Activity._
import org.scassandra.server.actors.ActivityLogActor._

class ActivityLogActorTest extends WordSpec with TestKitWithShutdown with Matchers
  with ImplicitSender {

  "activity log initially" must {
    val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))

    "have no stored queries" in {
      activityLog ! GetAllQueries
      expectMsg(Queries(List()))
    }

    "have no stored connections" in {
      activityLog ! GetAllConnections
      expectMsg(Connections(List()))
    }

    "have no stored prepares" in {
      activityLog ! GetAllPrepares
      expectMsg(Prepares(List()))
    }

    "have no stored prepared statement executions" in {
      activityLog ! GetAllExecutions
      expectMsg(Executions(List()))
    }

    "have no stored batch executions" in {
      activityLog ! GetAllBatches
      expectMsg(Batches(List()))
    }
  }

  "activity log recording connections" must {
    "record" in {
      val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))
      activityLog ! RecordConnection()
      activityLog ! GetAllConnections
      expectMsg(Connections(List(Connection())))
    }

    "clear" in {
      val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))
      activityLog ! RecordConnection()
      activityLog ! ClearConnections
      activityLog ! GetAllConnections
      expectMsg(Connections(List()))
    }
  }

  "activity log recording queries" must {
    val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))
    "record" in {
      val query = Query("select 1", Consistency.ONE, None)
      val query2 = Query("select 2", Consistency.ONE, None)
      activityLog ! RecordQuery(query)
      activityLog ! RecordQuery(query2)
      activityLog ! GetAllQueries
      expectMsg(Queries(List(query, query2)))
    }

    "clear" in {
      activityLog ! ClearQueries
      activityLog ! GetAllQueries
      expectMsg(Queries(List()))
    }
  }

  "activity log recording prepares" must {
    val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))
    "record" in {
      val prepare = PreparedStatementPreparation("1")
      val prepare2 = PreparedStatementPreparation("2")
      activityLog ! RecordPrepare(prepare)
      activityLog ! RecordPrepare(prepare2)
      activityLog ! GetAllPrepares
      expectMsg(Prepares(List(prepare, prepare2)))
    }

    "clear" in {
      activityLog ! ClearPrepares
      activityLog ! GetAllPrepares
      expectMsg(Prepares(List()))
    }
  }

  "activity log recording of ps executions" must {
    val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))
    "record" in {
      val execution = PreparedStatementExecution("select 1", Consistency.ONE, None, List(), List(), None)
      val execution2 = PreparedStatementExecution("select 2", Consistency.ONE, None, List(), List(), None)
      activityLog ! RecordExecution(execution)
      activityLog ! RecordExecution(execution2)
      activityLog ! GetAllExecutions
      expectMsg(Executions(List(execution, execution2)))
    }

    "clear" in {
      activityLog ! ClearExecutions
      activityLog ! GetAllExecutions
      expectMsg(Executions(List()))
    }
  }

  "activity log recording of batches" must {
    val activityLog = system.actorOf(Props(classOf[ActivityLogActor]))
    "record" in {
      val batch = BatchExecution(Seq(), Consistency.ONE, None, BatchType.UNLOGGED, None)
      val batch2 = BatchExecution(Seq(), Consistency.TWO, None, BatchType.UNLOGGED, None)
      activityLog ! RecordBatch(batch)
      activityLog ! RecordBatch(batch2)
      activityLog ! GetAllBatches
      expectMsg(Batches(List(batch, batch2)))
    }

    "clear" in {
      activityLog ! ClearBatches
      activityLog ! GetAllBatches
      expectMsg(Batches(List()))
    }
  }
}
