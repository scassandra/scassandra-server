package org.scassandra.server.actors

import akka.actor.{ActorRef, Actor}
import akka.util.ByteString
import org.scassandra.server.actors.BatchHandler.Execute
import org.scassandra.server.cqlmessages.CqlMessageFactory

class BatchHandler(tcpConnection: ActorRef, msgFactory: CqlMessageFactory) extends Actor {
  override def receive: Receive = {
    case Execute(body, stream) => tcpConnection ! msgFactory.createVoidMessage(stream)
  }
}

object BatchHandler {
  case class Execute(body: ByteString, stream: Byte)
}
