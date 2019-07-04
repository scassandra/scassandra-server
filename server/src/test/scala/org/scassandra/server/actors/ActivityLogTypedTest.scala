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

import akka.typed.ActorSystem
import akka.typed.testkit.scaladsl.TestProbe
import akka.typed.testkit.{ EffectfulActorContext, TestKitSettings }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }
import org.scassandra.codec.Consistency
import org.scassandra.server.actors.Activity.Query
import org.scassandra.server.actors.ActivityLogTyped._

import scala.language.postfixOps

class ActivityLogTypedTest extends WordSpec with BeforeAndAfterAll with ScalaFutures with Matchers {
  implicit private val al: ActorSystem[ActivityLogCommand] = ActorSystem(ActivityLogTyped.activityLog, "ActivityLogTest")
  implicit private val settings: TestKitSettings = TestKitSettings(al)
  private val testProbe: TestProbe[ActivityLogResponse] = TestProbe[ActivityLogResponse]()
  private val ctx = new EffectfulActorContext[ActivityLogCommand]("something", ActivityLogTyped.activityLog, 1, al)

  "activity log initially" must {
    "have no stored queries" in {
      ctx.run(GetQueries(testProbe.ref))
      testProbe.expectMsg(TQueries(List()))
    }
  }

  "activity log recording queries must" must {
    "store and return them" in {
      val query1 = Query("select 1", Consistency.ONE, None)
      val query2 = Query("select 2", Consistency.ONE, None)

      ctx.run(RecordQuery(query1, testProbe.ref))
      testProbe.expectMsg(Recorded)
      ctx.run(RecordQuery(query2, testProbe.ref))
      testProbe.expectMsg(Recorded)

      ctx.run(GetQueries(testProbe.ref))
      testProbe.expectMsg(TQueries(List(query1, query2)))
    }
  }

  override def afterAll(): Unit = {
    al.terminate()
  }
}

