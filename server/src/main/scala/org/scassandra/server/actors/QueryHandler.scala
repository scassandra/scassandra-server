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

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.util.ByteString
import org.scassandra.server.cqlmessages.{Consistency, CqlMessageFactory}
import org.scassandra.server.cqlmessages.CqlProtocolHelper._
import org.scassandra.server.priming._
import org.scassandra.server.priming.query.{Prime, PrimeMatch, PrimeQueryStore}

import scala.concurrent.duration.FiniteDuration

class QueryHandler(tcpConnection: ActorRef, primeQueryStore: PrimeQueryStore, msgFactory: CqlMessageFactory, activityLog: ActivityLog) extends Actor with ActorLogging {

  def receive = {
    case QueryHandler.Query(queryBody, stream) =>
      val iterator = queryBody.iterator
      // the first 4 bytes are an int which is the length of the query
      val queryLength = iterator.getInt
      val bodyAsBytes = new Array[Byte](queryLength)
      iterator.getBytes(bodyAsBytes)
      val queryText = new String(bodyAsBytes)
      val consistency = Consistency.fromCode(iterator.getShort)

      if (queryText.startsWith("use ")) {
        val keyspaceName: String = queryText.substring(4, queryLength)
        log.debug(s"Use keyspace $keyspaceName")
        sendMessage(None, tcpConnection, msgFactory.createSetKeyspaceMessage(keyspaceName, stream))
        activityLog.recordQuery(queryText, consistency)
      } else {
        val primeForIncomingQuery: Option[Prime] = primeQueryStore.get(PrimeMatch(queryText, consistency))
        primeForIncomingQuery match {
          case Some(prime) =>
            val message = prime.result match {
              case SuccessResult =>
                log.info(s"Found matching prime $prime for query $queryText")
                msgFactory.createRowsMessage(prime, stream)
              case result: ReadRequestTimeoutResult =>
                msgFactory.createReadTimeoutMessage(stream, consistency, result)
              case result: UnavailableResult =>
                msgFactory.createUnavailableMessage(stream, consistency, result)
              case result: WriteRequestTimeoutResult =>
                msgFactory.createWriteTimeoutMessage(stream, consistency, result)
            }
            sendMessage(prime.fixedDelay, tcpConnection, message)
            val queryRequest = msgFactory.parseQueryRequest(stream, queryBody, prime.variableTypes)
            log.info(s"Parsed query request $queryRequest")
            activityLog.recordQuery(queryRequest.query, consistency, queryRequest.parameters, prime.variableTypes)
          case None =>
            log.info(s"No prime found for $queryText")
            sendMessage(None, tcpConnection, msgFactory.createEmptyRowsMessage(stream))
            activityLog.recordQuery(queryText, consistency)
        }
      }
      log.info(s"Incoming query: $queryText at consistency: $consistency")
  }

  private def sendMessage(delay: Option[FiniteDuration], receiver: ActorRef, message: Any) = {
    delay match {
      case None => receiver ! message
      case Some(duration) =>
        log.info(s"Delaying response by $duration")
        context.system.scheduler.scheduleOnce(duration, receiver, message)(context.system.dispatcher)
    }
  }
}


object QueryHandler {
  case class Query(queryBody: ByteString, stream: Byte)
}


object QueryFlagParser {
  def hasFlag(flag : Byte, value : Byte) = {
    (value & flag) == flag
  }
}

object QueryFlags {
  val Values : Byte = (1 << 0).toByte
  val SkipMetadata : Byte = (1 << 1).toByte
  val PageSize : Byte = (1 << 3).toByte
  val PagingState : Byte = (1 << 4).toByte
  val SerialConsistency : Byte = (1 << 5).toByte
}

