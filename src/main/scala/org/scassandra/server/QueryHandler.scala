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
package org.scassandra.server

import akka.util.ByteString

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write
import org.scassandra.priming._
import scala.Some
import org.scassandra.cqlmessages.{CqlMessageFactory, Consistency}
import org.scassandra.priming.query.{PrimeMatch, PrimeQueryStore}

class QueryHandler(tcpConnection: ActorRef, primeQueryStore: PrimeQueryStore, msgFactory: CqlMessageFactory) extends Actor with Logging {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def receive = {
    case QueryHandlerMessages.Query(queryBody, stream) =>

      val iterator = queryBody.iterator
      // the first 4 bytes are an int which is the length of the query
      val queryLength = iterator.getInt
      logger.trace(s"Query length is $queryLength")
      val bodyAsBytes = new Array[Byte](queryLength)
      iterator.getBytes(bodyAsBytes)
      val queryText = new String(bodyAsBytes)
      val consistency = iterator.getShort
      logger.info(s"Incoming query: $queryText at consistency: $consistency }}")
      ActivityLog.recordQuery(queryText, Consistency.fromCode(consistency))
      if (queryText.startsWith("use ")) {
        val keyspaceName: String = queryText.substring(4, queryLength)
        logger.debug(s"Use keyspace $keyspaceName")
        tcpConnection ! Write(msgFactory.createSetKeyspaceMessage(keyspaceName, stream).serialize())
      } else {
        primeQueryStore.get(PrimeMatch(queryText, Consistency.fromCode(consistency))) match {
          case Some(prime) => {
            prime.result match {
              case Success => {
                logger.info(s"Found matching prime $prime for query $queryText")
                val message = msgFactory.createRowsMessage(prime, stream)
                logger.debug(s"Sending message: $message")
                tcpConnection ! Write(message.serialize())
              }
              case ReadTimeout => {
                tcpConnection ! Write(msgFactory.createReadTimeoutMessage(stream).serialize())
              }
              case Unavailable => {
                tcpConnection ! Write(msgFactory.createUnavailableMessage(stream).serialize())
              }
              case WriteTimeout => {
                tcpConnection ! Write(msgFactory.createWriteTimeoutMessage(stream).serialize())
              }
            }
          }
          case None => {
            logger.info(s"No prime found for $queryText")
            tcpConnection ! Write(msgFactory.createEmptyRowsMessage(stream).serialize())
          }
          case msg @ _ => {
            logger.debug(s"Received unexpected result back from primed results: $msg")
          }
        }
      }

    case message @ _ =>
      logger.debug(s"Received unknown message: $message")

  }
}

object QueryHandlerMessages {
  case class Query(queryBody: ByteString, stream: Byte)
}