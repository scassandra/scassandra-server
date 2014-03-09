package uk.co.scassandra.server

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.Logging
import akka.io.Tcp.Write
import akka.util.ByteString
import com.batey.narinc.client.cqlmessages.Ready

class RegisterHandler(connection: ActorRef) extends Actor with Logging {
  def receive = {
    case registerMsg @ RegisterHandlerMessages.Register(_) => {
      logger.info(s"Received register message ${registerMsg}")
      connection ! Write(Ready().serialize())
    }
    case msg @ _ => {
      logger.warn(s"Received unknown message ${msg}")
    }
  }
}

object RegisterHandlerMessages {
  case class Register(messageBody: ByteString)

}