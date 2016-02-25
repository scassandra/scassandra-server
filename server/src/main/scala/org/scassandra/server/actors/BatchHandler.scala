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

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.{ask, pipe}
import akka.util.{ByteString, Timeout}
import org.scassandra.server.actors.BatchHandler.{BatchToFinish, Execute}
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.response.Response
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming._
import org.scassandra.server.priming.batch.{BatchPrime, PrimeBatchStore}
import org.scassandra.server.priming.prepared.PreparedStoreLookup
import org.scassandra.server.priming.query.PrimeMatch

import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class BatchHandler(tcpConnection: ActorRef,
                   msgFactory: CqlMessageFactory,
                   activityLog: ActivityLog,
                   prepareHandler: ActorRef,
                   batchPrimeStore: PrimeBatchStore,
                   preparedStore: PreparedStoreLookup) extends Actor with ActorLogging {

  import CqlProtocolHelper._
  import context.dispatcher
  private implicit val timeout: Timeout = 1 second
  private implicit val protocolVersion = msgFactory.protocolVersion

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
        val execution: BatchExecution = BatchExecution(justQueries.toList, consistency, batchType)
        activityLog.recordBatchExecution(execution)
        sendResponse(execution, stream, consistency)
      } else {
        val ps: Future[BatchToFinish] = (prepareHandler ? PreparedStatementQuery(ids)).mapTo[PreparedStatementResponse]
          .map(result => BatchToFinish(statements, result.preparedStatementText, consistency, batchType, stream))
        log.debug("Piping batch to finish to self {}", ps)
        ps pipeTo self
      }

    case BatchToFinish(inFlight, ids, consistency, batchType, stream) =>
      log.debug("Received batch to finish {}", inFlight)
      val statements: Seq[BatchQuery] = inFlight.map( {
        //todo support capturing of variables for queries
        case NormalQuery(text) => BatchQuery(text, QueryKind)
        case IncompletePreparedStatement(id, variables: List[Array[Byte]]) =>
          val queryText: String = ids.getOrElse(id, "A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?")
          preparedStore.findPrime(PrimeMatch(queryText, consistency)) match {
            case Some(result) =>
              val variableTypes = result.variableTypes
              val parsedVariables = variables.zip(variableTypes).map { case (bytes, colType) => readValue(bytes, colType) }
              BatchQuery(queryText, PreparedStatementKind, parsedVariables)
            case None => BatchQuery(queryText, PreparedStatementKind)
          }

      })
      val execution: BatchExecution = BatchExecution(statements, consistency, batchType)
      activityLog.recordBatchExecution(execution)
      sendResponse(execution, stream, consistency)

    case msg @ _ => log.warning("Unknown msg {}", msg)
  }

  def readValue(bytes: Array[Byte], colType: ColumnType[_]): Option[Any] = {
    if (bytes.isEmpty) None else colType.readValue(ByteString(bytes).iterator)
  }

  private def sendResponse(execution: BatchExecution, stream: Byte, consistency: Consistency): Unit = {
    val prime: Option[BatchPrime] = batchPrimeStore.findPrime(execution)
    log.info("Found prime {} for batch execution {}", prime, execution)
    prime match {
      case Some(BatchPrime(SuccessResult, delay: Option[Long])) => sendWithDelay(msgFactory.createVoidMessage(stream), delay)
      case Some(BatchPrime(errorResult: ErrorResult, delay: Option[Long])) => sendWithDelay(msgFactory.createErrorMessage(errorResult, stream, consistency), delay)
      case Some(BatchPrime(fatalResult: FatalResult, delay: Option[Long])) => fatalResult.produceFatalError(tcpConnection)
      case None => tcpConnection ! msgFactory.createVoidMessage(stream)
    }
  }

  private def sendWithDelay(response: Response, delay: Option[Long]): Unit = {
    val delayDuration: FiniteDuration = FiniteDuration(delay.getOrElse(0L), TimeUnit.MILLISECONDS)
    context.system.scheduler.scheduleOnce(delayDuration, tcpConnection, response)(context.system.dispatcher)
  }

}



private sealed trait InFlightBatchQuery
private case class NormalQuery(text: String) extends InFlightBatchQuery
private case class IncompletePreparedStatement(id: Int, variables: List[Array[Byte]]) extends InFlightBatchQuery

object BatchHandler {
  private case class BatchToFinish(inFlightBatchQuery: Seq[InFlightBatchQuery], idsToText: Map[Int, String], consistency: Consistency, batchType: BatchType, stream: Byte)
  case class Execute(body: ByteString, stream: Byte)
}
