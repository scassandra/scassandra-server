package uk.co.scassandra.server

import akka.util.ByteString

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write
import uk.co.scassandra.priming._
import scala.Some
import uk.co.scassandra.cqlmessages.Consistency
import uk.co.scassandra.cqlmessages.response.CqlMessageFactory
import uk.co.scassandra.priming.query.{PrimeMatch, PrimeQueryStore}

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
      logger.debug(s"Handling {{ query }} with {{ consistency }} {{ $queryText }} {{ $consistency }}")
      ActivityLog.recordQuery(queryText, Consistency.fromCode(consistency))
      if (queryText.startsWith("use ")) {
        val keyspaceName: String = queryText.substring(4, queryLength)
        logger.debug(s"Handling {{ use statement }} for {{ keyspacename }} {{ $queryText }} {{ $keyspaceName }}")
        tcpConnection ! Write(msgFactory.createSetKeyspaceMessage(keyspaceName, stream).serialize())
      } else {
        primeQueryStore.get(PrimeMatch(queryText, Consistency.fromCode(consistency))) match {
          case Some(prime) => {
            prime.result match {
              case Success => {
                logger.debug(s"Handling {{ query }} with {{ rows }} {{ $queryText }} {{ $prime }}")
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
            logger.info("Sending void result")
            tcpConnection ! Write(msgFactory.createVoidMessage(stream).serialize())
          }
          case msg@_ => {
            logger.info(s"Received unexpected result back from primed results: $msg")
          }
        }
      }

    case message@_ =>
      logger.info(s"Received message: $message")

  }
}

object QueryHandlerMessages {
  case class Query(queryBody: ByteString, stream: Byte)
}