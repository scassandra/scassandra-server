package org.scassandra

import akka.actor.{ActorRef, Actor}
import com.typesafe.scalalogging.slf4j.Logging

class ServerReadyListener extends Actor with Logging {
  var serverReadyReceiver: ActorRef = null
  var alreadyReady = false

  def receive = {
        case OnServerReady => {
          serverReadyReceiver = sender
          if (alreadyReady) {
            logger.info("OnServerReady - ServerReady message already received. Sending ServerReady.")
            serverReadyReceiver ! ServerReady
          }
        }
        case ServerReady => {
          if (serverReadyReceiver != null) {
            logger.info("ServerReady - Forwarding ServerReady to listener.")
            serverReadyReceiver ! ServerReady
          } else {
            logger.info("ServerReady - No listener yet.")
            alreadyReady = true
          }
        }
        case msg @ _ => logger.info(s"Received unknown message $msg")
  }
}

case object ServerReady

case object OnServerReady
