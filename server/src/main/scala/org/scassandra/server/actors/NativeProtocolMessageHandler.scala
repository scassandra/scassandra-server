/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.actors

import akka.actor.{ActorRef, ActorRefFactory, ActorLogging, Actor}
import akka.io.Tcp.Write
import akka.util.ByteString
import org.scassandra.server.RegisterHandlerMessages
import org.scassandra.server.actors.NativeProtocolMessageHandler.Process
import org.scassandra.server.cqlmessages._

/*
 * Expects full native protocol messages.
 */
class NativeProtocolMessageHandler(queryHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        batchHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory, ActorRef) => ActorRef,
                        registerHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        optionsHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        prepareHandler: ActorRef,
                        executeHandler: ActorRef) extends Actor with ActorLogging {

  var messageFactory: CqlMessageFactory = _

  var registerHandler: ActorRef = _
  var queryHandler: ActorRef = _
  var batchHandler: ActorRef = _
  var optionsHandler: ActorRef = _
  var ready = false

  def receive: Receive = {
    case Process(opCode, stream, messageBody, protocolVersion) => {
      opCode match {
        case OpCodes.Startup =>
          log.info(s"Sending ready message to ${sender()}")
          initialiseMessageFactory(protocolVersion)
          queryHandler = queryHandlerFactory(context, sender(), messageFactory)
          batchHandler = batchHandlerFactory(context, sender(), messageFactory, prepareHandler)
          registerHandler = registerHandlerFactory(context, sender(), messageFactory)
          optionsHandler = optionsHandlerFactory(context, sender(), messageFactory)
          sender ! messageFactory.createReadyMessage(stream)
          ready = true
        case OpCodes.Query =>
          if (!ready) {
            initialiseMessageFactory(protocolVersion)
            log.info("Received query before startup message, sending error")
            sender ! Write(messageFactory.createQueryBeforeErrorMessage().serialize())
          } else {
            queryHandler ! QueryHandler.Query(messageBody, stream)
          }
        case OpCodes.Batch =>
          log.debug("Received register message. Sending to BatchHandler")
          batchHandler ! BatchHandler.Execute(messageBody, stream)
        case OpCodes.Register =>
          log.debug("Received register message. Sending to RegisterHandler")
          registerHandler ! RegisterHandlerMessages.Register(messageBody, stream)
        case OpCodes.Options =>
          log.debug("Received options message. Sending to OptionsHandler")
          optionsHandler ! OptionsHandlerMessages.OptionsMessage(stream)
        case OpCodes.Prepare =>
          log.debug("Received prepare message. Sending to PrepareHandler")
          prepareHandler ! PrepareHandler.Prepare(messageBody, stream, messageFactory, sender())
        case OpCodes.Execute =>
          log.debug("Received execute message. Sending to ExecuteHandler")
          executeHandler ! ExecuteHandler.Execute(messageBody, stream, messageFactory, sender())
        case opCode @ _ =>
          log.warning(s"Received unknown opcode $opCode this probably means this feature is yet to be implemented the message body is $messageBody")
      }
    }
  }


  def initialiseMessageFactory(protocolVersion: Byte) = {
    messageFactory = if (protocolVersion == ProtocolVersion.ClientProtocolVersionOne) {
      log.debug("Connection is for protocol version one")
      VersionOneMessageFactory
    } else {
      log.debug("Connection is for protocol version two")
      VersionTwoMessageFactory
    }
  }

}

object NativeProtocolMessageHandler {
  case class Process(opCode: Byte, stream: Byte, messageBody: ByteString, protocolVersion: Byte)
}
