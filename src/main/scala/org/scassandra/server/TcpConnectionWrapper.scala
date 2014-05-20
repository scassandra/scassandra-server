package org.scassandra.server

import akka.actor.{ActorRef, Actor}
import org.scassandra.cqlmessages.CqlMessage
import akka.io.Tcp.Write
import com.typesafe.scalalogging.slf4j.Logging

class TcpConnectionWrapper(tcpConnection : ActorRef) extends Actor with Logging {
  def receive: Actor.Receive = {
    case msg : CqlMessage => {
      tcpConnection ! Write(msg.serialize())
    }
    case _ @ msg => {
      logger.error(s"Received unknown msg $msg")
    }
  }
}
