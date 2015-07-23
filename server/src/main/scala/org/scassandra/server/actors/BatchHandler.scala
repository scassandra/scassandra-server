package org.scassandra.server.actors

import akka.actor.Actor
import akka.util.ByteString

class BatchHandler extends Actor {
  override def receive: Receive = {
    case _ =>
  }
}

object BatchHandler {
  case class Execute(body: ByteString, stream: Byte)
}
