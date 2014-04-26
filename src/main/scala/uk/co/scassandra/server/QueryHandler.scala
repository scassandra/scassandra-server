package uk.co.scassandra.server
import akka.util.ByteString

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write
import uk.co.scassandra.priming._
import scala.Some
import uk.co.scassandra.cqlmessages.Consistency
import uk.co.scassandra.cqlmessages.response.CqlMessageFactory

class QueryHandler(tcpConnection: ActorRef, primedResults : PrimedResults, msgFactory: CqlMessageFactory) extends Actor with Logging {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def receive = {
    case QueryHandlerMessages.Query(queryBody, stream) =>

      val iterator = queryBody.iterator
      // the first 4 bytes are an int which is the length of the query
      val queryLength = iterator.getInt
      logger.info(s"Query length is $queryLength")
      val bodyAsBytes = new Array[Byte](queryLength)
      iterator.getBytes(bodyAsBytes)
      val queryText = new String(bodyAsBytes)
      val consistency = iterator.getShort
      logger.info(s"Handling query |${queryText}| with consistency ${consistency}")
      ActivityLog.recordQuery(queryText, Consistency.fromCode(consistency))
      if (queryText.startsWith("use ")) {
        val keyspaceName: String = queryText.substring(4, queryLength)
        logger.info(s"Handling use statement $queryText for keyspacename |$keyspaceName|")
        tcpConnection ! Write(msgFactory.createSetKeyspaceMessage(keyspaceName, stream).serialize())
      } else {
        primedResults.get(PrimeMatch(queryText, Consistency.fromCode(consistency))) match {
          case Some(prime) => {
            prime.result match {
              case Success => {
                logger.info(s"Handling query ${queryText} with rows ${prime}")
                val bytesToSend: ByteString = msgFactory.createRowsMessage(prime, stream).serialize()
                logger.debug(s"Sending bytes ${bytesToSend}")
                tcpConnection ! Write(bytesToSend)
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
          case msg @ _ => {
            logger.error(s"Got unexpected result back from primed results ${msg}")
          }
        }
      }

    case message @ _ =>
      logger.info(s"Received message $message")

  }
}

object QueryHandlerMessages {
  case class Query(queryBody: ByteString, stream: Byte)
}