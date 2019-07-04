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

import akka.typed.{ ActorRef, Behavior }
import akka.typed.scaladsl.Actor
import org.scassandra.server.actors.Activity.Query

object ActivityLogTyped {

  val activityLog: Behavior[ActivityLogCommand] = activityLogState(DynamicActivityLog())

  def activityLogState(state: DynamicActivityLog): Behavior[ActivityLogCommand] = {
    Actor.immutable[ActivityLogCommand] { (ctx, msg) =>
      msg match {
        case GetQueries(sender) =>
          sender ! TQueries(state.get(QueryType))
          Actor.same
        case RecordQuery(query, sender) =>
          sender ! Recorded
          activityLogState(state.store(QueryType)(query))
      }
    }
  }

  sealed trait ActivityLogCommand
  final case class GetQueries(replyTo: ActorRef[TQueries]) extends ActivityLogCommand
  final case class RecordQuery(query: Query, replyTo: ActorRef[ActivityLogResponse]) extends ActivityLogCommand

  sealed trait ActivityLogResponse
  case class TQueries(queries: List[Query]) extends ActivityLogResponse

  final case object Recorded extends ActivityLogResponse

  private[actors] sealed trait ActivityType {
    type RecordType
  }

  private[actors] case object QueryType extends ActivityType {
    type RecordType = Query
  }

  class DynamicActivityLog private (data: Map[ActivityType, List[Any]]) {
    def get(key: ActivityType): List[key.RecordType] =
      data.getOrElse(key, List.empty[key.RecordType]).asInstanceOf[List[key.RecordType]].reverse
    def store(key: ActivityType)(value: key.RecordType): DynamicActivityLog = {
      val x: List[Any] = value :: data.getOrElse(key, List())
      DynamicActivityLog(data + (key -> x))
    }
  }

  object DynamicActivityLog {
    def apply(data: Map[ActivityType, List[Any]] = Map[ActivityType, List[Any]]()): DynamicActivityLog = new DynamicActivityLog(data)
  }
}
