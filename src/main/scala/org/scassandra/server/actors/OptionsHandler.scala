package org.scassandra.server.actors

import akka.actor.{ActorRef, Actor}
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.server.cqlmessages.CqlMessageFactory

class OptionsHandler(connection: ActorRef, msgFactory: CqlMessageFactory) extends Actor with Logging {
  import org.scassandra.server.actors.OptionsHandlerMessages._

  override def receive: Receive = {
    case msg @ OptionsMessage(stream) => {
      logger.debug(s"Received OPTIONS message $msg")
      connection ! msgFactory.createSupportedMessage(stream)
    }
    case msg @ _ => {
      logger.debug(s"Received unknown message $msg")
    }
  }
}

object OptionsHandlerMessages {
  case class OptionsMessage(stream: Byte)
}
