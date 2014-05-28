package org.scassandra

import akka.actor.{ActorRef, Actor}
import com.typesafe.scalalogging.slf4j.Logging

class ServerReadyListener extends Actor with Logging {
  var serverReadyReceiver: ActorRef = null

  def receive = {
        case OnServerReady => serverReadyReceiver = sender
        case ServerReady => if (serverReadyReceiver != null) serverReadyReceiver ! ServerReady
        case msg @ _ => logger.info(s"Received unknown message $msg")
  }
}

case object ServerReady

case object OnServerReady
