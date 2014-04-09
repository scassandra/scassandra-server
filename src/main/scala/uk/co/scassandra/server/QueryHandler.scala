package uk.co.scassandra.server
import akka.util.ByteString

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write
import uk.co.scassandra.priming._
import org.scassandra.cqlmessages.response._
import org.scassandra.cqlmessages.response.ReadRequestTimeout
import org.scassandra.cqlmessages.response.VoidResult
import org.scassandra.cqlmessages.response.Row
import org.scassandra.cqlmessages.response.SetKeyspace
import org.scassandra.cqlmessages.response.UnavailableException
import org.scassandra.cqlmessages.response.Rows
import scala.Some
import org.scassandra.cqlmessages.{Consistency, ONE}

class QueryHandler(tcpConnection: ActorRef, primedResults : PrimedResults) extends Actor with Logging {
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
        tcpConnection ! Write(SetKeyspace(keyspaceName, stream).serialize())
      } else {
        primedResults.get(When(queryText)) match {
          case Some(prime) => {
            prime.result match {
              case Success => {
                logger.info(s"Handling query ${queryText} with rows ${prime}")
                val bytesToSend: ByteString = Rows("", "", stream, prime.columnTypes, prime.rows.map(row => Row(row))).serialize()
                logger.debug(s"Sending bytes ${bytesToSend}")
                tcpConnection ! Write(bytesToSend)
              }
              case ReadTimeout => {
                tcpConnection ! Write(ReadRequestTimeout(stream).serialize())
              }
              case Unavailable => {
                tcpConnection ! Write(UnavailableException(stream).serialize())
              }
              case WriteTimeout => {
                tcpConnection ! Write(WriteRequestTimeout(stream).serialize())
              }
            }
          }
          case None => {
            logger.info("Sending void result")
            tcpConnection ! Write(VoidResult(stream).serialize())
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