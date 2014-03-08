package uk.co.scassandra.server
import akka.util.ByteString

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write

class QueryHandler(tcpConnection: ActorRef) extends Actor with Logging {
  def receive = {
    case QueryHandlerMessages.Query(queryString, stream) =>
      logger.info(s"Handling query |${queryString.utf8String}|")

      // the first 4 bytes are an int which is the length of the query
      val queryLength = queryString.take(4).asByteBuffer.getInt
      logger.info(s"Query length is $queryLength")
      val rest = queryString.drop(4)

      if (rest.startsWith("use ")) {
        val query = rest.utf8String
        val keyspaceName: String = query.substring(4, queryLength)
        logger.info(s"Handling use statement $query for keyspacename |$keyspaceName|")
        tcpConnection ! Write(SetKeyspace(keyspaceName, stream).serialize())
      } else {
        // TODO - [DN] Update code so that query results are returned with the correct Result Kind instead of VoidResult
        logger.info("Sending void result")
        tcpConnection ! Write(VoidResult(stream).serialize())
      }

    case message@_ =>
      logger.info(s"Received message $message")

  }
}

object QueryHandlerMessages {

  case class Query(queryString: ByteString, stream: Byte)

}