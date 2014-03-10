package uk.co.scassandra.server
import akka.util.ByteString

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write
import com.batey.narinc.client.cqlmessages.{Row, Rows, VoidResult, SetKeyspace}
import uk.co.scassandra.priming.PrimedResults

class QueryHandler(tcpConnection: ActorRef, primedResults : PrimedResults) extends Actor with Logging {
  def receive = {
    case QueryHandlerMessages.Query(queryBody, stream) =>

      // the first 4 bytes are an int which is the length of the query
      val queryLength = queryBody.take(4).asByteBuffer.getInt
      logger.info(s"Query length is $queryLength")
      val queryText = queryBody.drop(4).take(queryLength)
      logger.info(s"Handling query |${queryText.utf8String}|")

      if (queryText.startsWith("use ")) {
        val query = queryText.utf8String
        val keyspaceName: String = query.substring(4, queryLength)
        logger.info(s"Handling use statement $query for keyspacename |$keyspaceName|")
        tcpConnection ! Write(SetKeyspace(keyspaceName, stream).serialize())
      } else {
        // TODO - [DN] Update code so that query results are returned with the correct Result Kind instead of VoidResult
        primedResults.get(queryText.utf8String) match {
          case Some(rows) => {
            logger.info(s"Handling query ${queryText.utf8String} with rows ${rows}")
            val columnNames = rows.flatMap(row => row.map( colAndValue => colAndValue._1 )).distinct
            val bytesToSend: ByteString = Rows("", "", stream, columnNames, rows.map(row => Row(row))).serialize()
            logger.debug(s"Sending bytes ${bytesToSend}")
            tcpConnection ! Write(bytesToSend)
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

    case message@_ =>
      logger.info(s"Received message $message")

  }
}

object QueryHandlerMessages {
  case class Query(queryString: ByteString, stream: Byte)
}