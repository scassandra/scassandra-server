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
package org.scassandra.server.actors

import akka.actor.{ActorLogging, ActorRef, Actor}
import akka.util.{Timeout, ByteString}
import akka.pattern.{ask, pipe}

import org.scassandra.server.actors.BatchHandler.{BatchToFinish, Execute}
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementResponse, PreparedStatementQuery}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.priming.{BatchExecution, BatchQuery, ActivityLog}

import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class BatchHandler(tcpConnection: ActorRef,
                   msgFactory: CqlMessageFactory,
                   activityLog: ActivityLog,
                   prepareHandler: ActorRef) extends Actor with ActorLogging {

  import CqlProtocolHelper._
  import context.dispatcher
  implicit val timeout: Timeout = 1 second

  def receive: Receive = {
    case Execute(body, stream) =>
      val iterator = body.iterator
      val batchType = BatchType.fromCode(iterator.getByte)
      val numStatements = iterator.getShort

      val statements: IndexedSeq[InFlightBatchQuery] = (0 until numStatements).map(_ => {
        BatchQueryKind.fromCode(iterator.getByte) match {
          case QueryKind =>
            NormalQuery(msgFactory.parseBatchQuery(iterator))
          case PreparedStatementKind =>
            val id = readPreparedStatementId(iterator)
            val variables = msgFactory.readVariables(iterator)
            IncompletePreparedStatement(id, variables)
        }
      })
      val consistency = Consistency.fromCode(iterator.getShort)
      val ids: List[Int] = statements.collect { case IncompletePreparedStatement(id, _) => id }.toList
      log.debug("Prepared statement ids {}", ids)

      if (ids.isEmpty) {
        val justQueries = statements.map(q => BatchQuery(q.asInstanceOf[NormalQuery].text, QueryKind))
        activityLog.recordBatchExecution(BatchExecution(justQueries.toList, consistency, batchType))
        tcpConnection ! msgFactory.createVoidMessage(stream)
      } else {
        val ps: Future[BatchToFinish] = (prepareHandler ? PreparedStatementQuery(ids)).mapTo[PreparedStatementResponse]
          .map(result => BatchToFinish(statements, result.preparedStatementText, consistency, batchType, stream))
        log.debug("Piping batch to finish to self {}", ps)
        ps pipeTo self
      }

    case BatchToFinish(inFlight, ids, consistency, batchType, stream) =>
      log.debug("Received batch to finish {}", inFlight)
      val statements: Seq[BatchQuery] = inFlight.map( {
        case NormalQuery(text) => BatchQuery(text, QueryKind)
        case IncompletePreparedStatement(id, _) => BatchQuery(ids.getOrElse(id, "A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?"), PreparedStatementKind)
      })
      activityLog.recordBatchExecution(BatchExecution(statements, consistency, batchType))
      tcpConnection ! msgFactory.createVoidMessage(stream)

    case msg @ _ => log.warning("Unknown msg {}", msg)
  }

}


private sealed trait InFlightBatchQuery
private case class NormalQuery(text: String) extends InFlightBatchQuery
private case class IncompletePreparedStatement(id: Int, variables: List[Array[Byte]]) extends InFlightBatchQuery

object BatchHandler {
  //todo stop this being a map of options lol
  private case class BatchToFinish(inFlightBatchQuery: Seq[InFlightBatchQuery], idsToText: Map[Int, String], consistency: Consistency, batchType: BatchType, stream: Byte)
  case class Execute(body: ByteString, stream: Byte)
}
