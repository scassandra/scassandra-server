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
import org.scassandra.server.actors.BatchHandler.BatchToFinish
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.priming._
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.prepared.PreparedStoreLookup
import org.scassandra.server.priming.query.Reply

import scala.concurrent.duration._
import scala.language.postfixOps

class BatchHandler(activityLog: ActivityLog,
                   prepareHandler: ActorRef,
                   batchPrimeStore: PrimeBatchStore,
                   preparedStore: PreparedStoreLookup) extends ProtocolActor {

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
        log.debug("Piping batch to finish to self")
        toFinish pipeTo self
      }

    case BatchToFinish(header, batch, preparedResponse, connection) =>
      log.debug("Received batch to finish")
      val batchQueries = batch.queries.map {
        case SimpleBatchQuery(query, _) => BatchQuery(query, BatchQueryKind.Simple)
        case PreparedBatchQuery(i, byteValues) =>
          val id = i.toInt()
          preparedResponse.prepared.get(id) match {
            case Some((queryText, prepared)) =>
              // Decode query parameters using the prepared statement metadata.
              // TODO: Decoding values from metadata is a common pattern, DRY
              val dataTypes = prepared.resultMetadata.columnSpec.getOrElse(Nil).map(_.dataType)
              val values = byteValues.zip(dataTypes).map {
                case ((Bytes(bytes), dataType)) => dataType.codec.decodeValue(bytes.toBitVector).require // TODO: handle decode failure
                case ((_, dataType)) => null // TODO: Handle Null and Unset case.
              }
              BatchQuery(queryText, BatchQueryKind.Prepared, values)
            case None => BatchQuery(
              "A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?",
              BatchQueryKind.Prepared)
          }
      }
      processBatch(header, batch, batchQueries, connection)
  }

  def processBatch(header: FrameHeader, batch: Batch, batchQueries: Seq[BatchQuery], recipient: ActorRef) = {
    val execution = BatchExecution(batchQueries, batch.consistency, batch.batchType)
    activityLog.recordBatchExecution(execution)
    val prime = batchPrimeStore(execution)
    prime.foreach(p => log.info("Found prime {} for batch execution {}", p, execution))
    writePrime(batch, prime, header, recipient=Some(recipient), alternative = Some(Reply(VoidResult)))
  }
}

object BatchHandler {
  case class BatchToFinish(header: FrameHeader, batch: Batch, prepared: PreparedStatementResponse, connection: ActorRef)
}
