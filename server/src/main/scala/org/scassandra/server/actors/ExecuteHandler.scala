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

import akka.actor.ActorRef
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.actors.Activity.PreparedStatementExecution
import org.scassandra.server.actors.ActivityLogActor.RecordExecution
import org.scassandra.server.actors.ExecuteHandler.HandleExecute
import org.scassandra.server.actors.PrepareHandler.{ PreparedStatementQuery, PreparedStatementResponse }
import org.scassandra.server.actors.ProtocolActor._
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{ LookupByExecute, PrimeMatch }
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{ Prime, Reply }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{ Failure, Success }

class ExecuteHandler(primePreparedStore: ActorRef, activityLog: ActorRef, prepareHandler: ActorRef) extends ProtocolActor {

  import context.dispatcher

  implicit val timeout: Timeout = 1 second

  def receive: Receive = {
    case ProtocolMessage(Frame(header, e: Execute)) =>
      val id = e.id.toInt()
      val recipient = sender
      val executeRequest = (prepareHandler ? PreparedStatementQuery(List(id)))
        .mapTo[PreparedStatementResponse]
        .map(res => HandleExecute(res.prepared.get(id), header, e, recipient))

      executeRequest.pipeTo(self)

    case HandleExecute(query, header, execute, connection) =>
      handleExecute(query, header, execute, connection)
  }

  def handleExecute(preparedStatement: Option[(String, Prepared)], header: FrameHeader, execute: Execute, connection: ActorRef): Unit = {
    implicit val protocolVersion: ProtocolVersion = header.version.version
    preparedStatement match {
      case Some((queryText, prepared)) =>
        val prime2: Future[Option[Prime]] =
          (primePreparedStore ? LookupByExecute(queryText, execute, protocolVersion)).mapTo[PrimeMatch]
            .map(_.prime)

        // Decode query parameters using the prepared statement metadata.
        val dataTypes: List[DataType] = prepared.preparedMetadata.columnSpec.map(_.dataType)
        val values: Option[List[Any]] = extractQueryVariables(queryText, execute.parameters.values.map(_.map(_.value)), dataTypes)

        prime2.onComplete {
          case Success(None) =>
            recordExecution(queryText, execute, dataTypes, values)
            writePrime(execute, None, header, connection, alternative = Some(Reply(VoidResult)), consistency = Some(execute.parameters.consistency))(context.system)

          case Success(somePrime) =>
            recordExecution(queryText, execute, dataTypes, values)
            writePrime(execute, somePrime, header, connection, alternative = Some(Reply(VoidResult)), consistency = Some(execute.parameters.consistency))(context.system)

          case Failure(e) =>
            log.warning(s"Unable to get prime for statement $queryText. Your client will get a timeout as no response will be sent", e)
        }

      case None =>
        val errMsg = s"Could not find prepared statement with id: 0x${execute.id.toHex}"
        activityLog ! RecordExecution(PreparedStatementExecution(errMsg, execute.parameters.consistency,
          execute.parameters.serialConsistency, Nil, Nil, execute.parameters.timestamp))
        val unprepared = Unprepared(errMsg, execute.id)
        write(unprepared, header, connection)
    }
  }

  private def recordExecution(queryText: String, execute: Execute, dataTypes: List[DataType], values: Option[List[Any]]): Unit = {
    values match {
      case Some(v) =>
        activityLog ! RecordExecution(PreparedStatementExecution(queryText, execute.parameters.consistency,
          execute.parameters.serialConsistency, v, dataTypes, execute.parameters.timestamp))
      case None =>
        activityLog ! RecordExecution(PreparedStatementExecution(queryText, execute.parameters.consistency,
          execute.parameters.serialConsistency, Nil, Nil, execute.parameters.timestamp))
    }
  }
}

object ExecuteHandler {
  case class HandleExecute(query: Option[(String, Prepared)], header: FrameHeader, execute: Execute, connection: ActorRef)
}
