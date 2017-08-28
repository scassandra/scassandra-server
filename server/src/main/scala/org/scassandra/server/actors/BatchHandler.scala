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

import akka.actor.ActorRef
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.scassandra.codec._
import org.scassandra.codec.messages.{BatchQueryKind, PreparedBatchQuery, SimpleBatchQuery}
import org.scassandra.server.actors.Activity._
import org.scassandra.server.actors.ActivityLogActor.RecordBatch
import org.scassandra.server.actors.BatchHandler.BatchToFinish
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.actors.ProtocolActor._
import org.scassandra.server.actors.priming.PrimeBatchStoreActor.{MatchBatch, MatchResult}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.Reply

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class BatchHandler(activityLog: ActorRef,
                   prepareHandler: ActorRef,
                   batchPrimeStore: ActorRef) extends ProtocolActor {

  import context.dispatcher
  private implicit val timeout: Timeout = 1 second

  def receive: Receive = {
    case ProtocolMessage(Frame(header, batch: Batch)) =>
      val preparedIds = batch.queries.collect {
        case PreparedBatchQuery(id, _) => id.toInt()
      }
      log.debug("Prepared statement ids {}", preparedIds)

      if(preparedIds.isEmpty) {
        // No prepared statements, process batch as is.
        val simpleQueries = batch.queries.collect {
          // TODO: The values aren't actually read, but we don't know the type anyways so not much we can do there.
          case SimpleBatchQuery(query, _) => BatchQuery(query, BatchQueryKind.Simple)
        }
        processBatch(header, batch, simpleQueries, sender)
      } else {
        // We have prepared statements in the batch, fetch their associated metadata and send another message
        // to self.
        val connection = sender()
        val toFinish = (prepareHandler ? PreparedStatementQuery(preparedIds)).mapTo[PreparedStatementResponse]
          .map(result => BatchToFinish(header, batch, result, connection))
        log.error("Piping batch to finish to self")
        toFinish pipeTo self
      }

    case BatchToFinish(header, batch, preparedResponse, connection) =>
      log.error("Received batch to finish")
      val batchQueries = batch.queries.map {
        case SimpleBatchQuery(query, _) => BatchQuery(query, BatchQueryKind.Simple)
        case PreparedBatchQuery(i, byteValues) =>
          val id = i.toInt()
          implicit val protocolVersion: ProtocolVersion = header.version.version
          preparedResponse.prepared.get(id) match {
            case Some((queryText, prepared)) =>
              // Decode query parameters using the prepared statement metadata.
              val dataTypes = prepared.preparedMetadata.columnSpec.map(_.dataType)
              val values = extractQueryVariables(queryText, Some(byteValues), dataTypes).getOrElse(Nil)
              BatchQuery(queryText, BatchQueryKind.Prepared, values, dataTypes)
            case None => BatchQuery(
              "A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?",
              BatchQueryKind.Prepared)
          }
      }
      processBatch(header, batch, batchQueries, connection)
  }

  def processBatch(header: FrameHeader, batch: Batch, batchQueries: Seq[BatchQuery], recipient: ActorRef): Unit = {
    val execution = BatchExecution(batchQueries, batch.consistency, batch.serialConsistency, batch.batchType, batch.timestamp)
    activityLog ! RecordBatch(execution)
    log.error("Getting prime for batch")
    (batchPrimeStore ? MatchBatch(execution)).mapTo[MatchResult].onComplete {
      case Failure(e) =>
        log.error("Failed to get response from batch prime store", e)
      case Success(MatchResult(prime)) =>
        writePrime(batch, prime, header, recipient, alternative = Some(Reply(VoidResult)), consistency = Some(batch.consistency))(context.system)
    }
  }
}

object BatchHandler {
  private case class BatchToFinish(header: FrameHeader, batch: Batch, prepared: PreparedStatementResponse, connection: ActorRef)
}
