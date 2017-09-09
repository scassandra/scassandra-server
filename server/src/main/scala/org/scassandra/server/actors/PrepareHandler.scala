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
package org.scassandra.server.actors

import akka.actor.{ Actor, ActorLogging, ActorRef }
import akka.pattern.ask
import akka.util.Timeout
import org.scassandra.codec._
import org.scassandra.server.actors.Activity.PreparedStatementPreparation
import org.scassandra.server.actors.ActivityLogActor.RecordPrepare
import org.scassandra.server.actors.PrepareHandler.{ PreparedStatementQuery, PreparedStatementResponse }
import org.scassandra.server.actors.ProtocolActor._
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{ LookupByPrepare, PrimeMatch }
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{ Fatal, Prime, Reply }
import org.scassandra.server.priming.prepared.PreparedStoreLookup

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{ Failure, Success }

// todo switch to using become
class PrepareHandler(primePreparedStore: ActorRef, activityLog: ActorRef) extends ProtocolActor with ActorLogging {

  import context.dispatcher

  private var nextId: Int = 1
  private var idToStatement: Map[Int, (String, Prepared)] = Map()
  implicit private val timeout: Timeout = Timeout(250 millis)

  def receive: Actor.Receive = {
    case ProtocolMessage(Frame(header, p: Prepare)) =>
      activityLog ! RecordPrepare(PreparedStatementPreparation(p.query))
      handlePrepare(header, p)
    case PreparedStatementQuery(ids) =>
      sender() ! PreparedStatementResponse(ids.flatMap(id => idToStatement.get(id) match {
        case Some(p) => Seq(id -> p)
        case None => Seq()
      }) toMap)
  }

  private def handlePrepare(header: FrameHeader, prepare: Prepare) = {
    val toReply = sender()
    val nextId = genNext()

    val awaitingPrime: Future[Prime] =
      (primePreparedStore ? LookupByPrepare(prepare, nextId))
        .mapTo[PrimeMatch]
        .map(_.prime.getOrElse(PreparedStoreLookup.defaultPrepared(prepare, nextId)))

    awaitingPrime.onComplete {
      case Success(prime: Prime) =>
        prime match {
          case Reply(p: Prepared, _, _) =>
            idToStatement += (p.id.toInt() -> (prepare.query, p))
            log.info(s"Prepared Statement has been prepared: |$prepare.query|. Prepared result is: $p")
          case Reply(m: Message, _, _) =>
            log.info(s"Got non-prepared response for query: |$prepare.query|. Result was: $m")
          case f: Fatal =>
            log.info(s"Got non-prepared response for query: |$prepare.query|. Result was: $f")
        }
        writePrime(prepare, Some(prime), header, toReply)(context.system)

      case Failure(e) =>
      //        log.warning("Error getting prime for prepare msg", e)
    }
  }

  private def genNext(): Int = {
    val next = nextId
    nextId += 1
    next
  }
}

object PrepareHandler {
  case class PreparedStatementQuery(id: List[Int])
  case class PreparedStatementResponse(prepared: Map[Int, (String, Prepared)])
}