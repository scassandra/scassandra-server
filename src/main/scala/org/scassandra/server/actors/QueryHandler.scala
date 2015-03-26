package org.scassandra.server.actors

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.server.cqlmessages.{Consistency, CqlMessageFactory, CqlProtocolHelper}
import org.scassandra.server.priming._
import org.scassandra.server.priming.query.{Prime, PrimeMatch, PrimeQueryStore}

import scala.concurrent.duration.FiniteDuration

class QueryHandler(tcpConnection: ActorRef, primeQueryStore: PrimeQueryStore, msgFactory: CqlMessageFactory, activityLog: ActivityLog) extends Actor with Logging {

  implicit val byteOrder = CqlProtocolHelper.byteOrder

  def receive = {
    case QueryHandlerMessages.Query(queryBody, stream) =>

      val iterator = queryBody.iterator
      // the first 4 bytes are an int which is the length of the query
      val queryLength = iterator.getInt
      logger.trace(s"Query length is ${queryLength}")
      val bodyAsBytes = new Array[Byte](queryLength)
      iterator.getBytes(bodyAsBytes)
      val queryText = new String(bodyAsBytes)
      val consistency = Consistency.fromCode(iterator.getShort)
      logger.info(s"Incoming query: $queryText at consistency: $consistency")
      activityLog.recordQuery(queryText, consistency)
      if (queryText.startsWith("use ")) {
        val keyspaceName: String = queryText.substring(4, queryLength)
        logger.debug(s"Use keyspace $keyspaceName")
        sendMessage(None, tcpConnection, msgFactory.createSetKeyspaceMessage(keyspaceName, stream))
      } else {
        val primeForIncomingQuery: Option[Prime] = primeQueryStore.get(PrimeMatch(queryText, consistency))
        primeForIncomingQuery match {
          case Some(prime) =>
            val message = prime.result match {
                //todo errors
              case SuccessResult =>
                logger.info(s"Found matching prime $prime for query $queryText")
                msgFactory.createRowsMessage(prime, stream)
              case result: ReadRequestTimeoutResult =>
                msgFactory.createReadTimeoutMessage(stream, consistency, result)
              case result: UnavailableResult =>
                msgFactory.createUnavailableMessage(stream, consistency, result)
              case result: WriteRequestTimeoutResult =>
                msgFactory.createWriteTimeoutMessage(stream, consistency, result)
            }
            sendMessage(prime.fixedDelay, tcpConnection, message)
          case None =>
            logger.info(s"No prime found for $queryText")
            sendMessage(None, tcpConnection, msgFactory.createEmptyRowsMessage(stream))
          case msg @ _ =>
            logger.debug(s"Received unexpected result back from primed results: $msg")
        }
      }
    case message @ _ =>
      logger.debug(s"Received unknown message: $message")
  }

  private def sendMessage(delay: Option[FiniteDuration], receiver: ActorRef, message: Any) = {

    delay match {
      case None => receiver ! message
      case Some(duration) => {
        logger.info(s"Delaying response by $duration")
        context.system.scheduler.scheduleOnce(duration, receiver, message)(context.system.dispatcher)
      }
    }
  }

}
