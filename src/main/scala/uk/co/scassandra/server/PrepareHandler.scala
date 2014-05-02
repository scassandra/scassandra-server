package uk.co.scassandra.server

import com.typesafe.scalalogging.slf4j.{Logging}
import akka.actor.{ActorRef, Actor}
import akka.util.ByteString
import uk.co.scassandra.cqlmessages.response.{CqlMessageFactory}
import uk.co.scassandra.cqlmessages.{ColumnType, CqlVarchar, CqlProtocolHelper}

class PrepareHandler() extends Actor with Logging {

  var sequence : Int = 1

  def receive: Actor.Receive = {
    case PrepareHandlerMessages.Prepare(body, stream, msgFactory, connection) => {
      logger.debug(s"Received prepare message $body")
      val query = CqlProtocolHelper.readLongString(body.iterator)
      logger.debug(s"Prepare for query $query")
      val numberOfParameters = query.toCharArray.filter(_ == '?').size
      val columnTypes: Map[String, ColumnType] = (0 until numberOfParameters).map(num => (num.toString,CqlVarchar)).toMap
      val preparedResult = msgFactory.createPreparedResult(stream, sequence, columnTypes)
      sequence = sequence+1
      logger.debug(s"Sending back prepared result ${preparedResult}")
      connection ! preparedResult
    }
    case PrepareHandlerMessages.Execute(body, stream, msgFactory, connection) => {
      logger.debug(s"Received execute message $body")
      val rowsMessage = msgFactory.createVoidMessage(stream)
      connection ! rowsMessage
    }
    case msg @ _ => {
      logger.debug(s"Received message $msg")
    }
  }
}

object PrepareHandlerMessages {
  case class Prepare(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
  case class Execute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
}
