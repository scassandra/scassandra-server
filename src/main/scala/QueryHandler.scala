import akka.actor.{Actor, ActorRef}
import akka.io.Tcp.Write
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging

class QueryHandler(tcpConnection : ActorRef) extends Actor with Logging {
  def receive = {
    case QueryHandlerMessages.Query(queryString : ByteString) => {
      logger.info(s"Handling query ${queryString}")
      if (queryString.startsWith("use ")) {
        val query = queryString.utf8String
        val keyspaceName: String = query.substring(4, query.length)
        logger.info(s"Handling use statement ${query} for keyspacename |${keyspaceName}|")
        tcpConnection ! Write(SetKeyspace(keyspaceName).serialize())
      } else {
        logger.info("Sending void result")
        tcpConnection ! Write(VoidResult.serialize())
      }
    }
    case message @ _ => {
      logger.info(s"Received message ${message}")
    }
  }
}

object QueryHandlerMessages {
  case class Query(queryString : ByteString)
}
