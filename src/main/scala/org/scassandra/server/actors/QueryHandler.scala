package org.scassandra.server.actors

import akka.actor.{ActorLogging, Actor, ActorRef}
import org.scassandra.server.cqlmessages.{Consistency, CqlMessageFactory, CqlProtocolHelper}
import org.scassandra.server.priming._
import org.scassandra.server.priming.query.{Prime, PrimeMatch, PrimeQueryStore}

import scala.concurrent.duration.FiniteDuration

class QueryHandler(tcpConnection: ActorRef, primeQueryStore: PrimeQueryStore, msgFactory: CqlMessageFactory, activityLog: ActivityLog) extends Actor with ActorLogging {

  implicit val byteOrder = CqlProtocolHelper.byteOrder

  def receive = {
    case QueryHandlerMessages.Query(queryBody, stream) =>
      val iterator = queryBody.iterator
      // the first 4 bytes are an int which is the length of the query
      val queryLength = iterator.getInt
      val bodyAsBytes = new Array[Byte](queryLength)
      iterator.getBytes(bodyAsBytes)
      val queryText = new String(bodyAsBytes)
      val consistency = Consistency.fromCode(iterator.getShort)
      log.info(s"Incoming query: $queryText at consistency: $consistency")
      activityLog.recordQuery(queryText, consistency)
      if (queryText.startsWith("use ")) {
        val keyspaceName: String = queryText.substring(4, queryLength)
        log.debug(s"Use keyspace $keyspaceName")
        sendMessage(None, tcpConnection, msgFactory.createSetKeyspaceMessage(keyspaceName, stream))
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
          case None =>
            log.info(s"No prime found for $queryText")
            sendMessage(None, tcpConnection, msgFactory.createEmptyRowsMessage(stream))
        }
      }
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
