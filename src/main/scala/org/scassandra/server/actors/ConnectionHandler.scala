/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
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

import akka.actor.{ActorLogging, Actor, ActorRef, ActorRefFactory}
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.server.RegisterHandlerMessages
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.response.UnsupportedProtocolVersion
import org.scassandra.server.priming.QueryHandlerMessages

/*
 * TODO: This class is on the verge of needing split up.
 *
 * This class's responsibility is:
 *  * To read full CQL messages from the data, knowing that they may not come all at once or that multiple
 *    messages may come in the same data packet
 *  * To check the opcode and forward to the correct handler
 *
 *  This could be changed to only know how to read full messages and then pass to an actor
 *  per stream that could check the opcode and forward.
 */
class ConnectionHandler(queryHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        registerHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        optionsHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        prepareHandler: ActorRef,
                        connectionWrapperFactory: (ActorRefFactory, ActorRef) => ActorRef) extends Actor with ActorLogging {

  import akka.io.Tcp._

  var ready = false
  var partialMessage = false
  var dataFromPreviousMessage: ByteString = _
  var currentData: ByteString = _
  var messageFactory: CqlMessageFactory = _
  var registerHandler: ActorRef = _
  var queryHandler: ActorRef = _
  var optionsHandler: ActorRef = _

  val ProtocolOneOrTwoHeaderLength = 8

  def receive = {
    case Received(data: ByteString) =>
      currentData = data
      if (partialMessage) {
        currentData = dataFromPreviousMessage ++ data
      }

      val messageLength = currentData.length

      // the header could be 8 or 9 bits now :(
      while (currentData.length >= ProtocolOneOrTwoHeaderLength && takeMessage()) {}

      if (currentData.length > 0) {
        partialMessage = true
        dataFromPreviousMessage = currentData
        currentData = ByteString()
      }
    case PeerClosed =>
      log.info("Client disconnected.")
      context stop self
    case unknown@_ =>
      log.warning(s"Unknown message $unknown")

  }

  private def processMessage(opCode: Byte, stream: Byte, messageBody: ByteString, protocolVersion: Byte) = {
    opCode match {
      case OpCodes.Startup =>
        log.debug("Sending ready message")
        initialiseMessageFactory(protocolVersion)
        val wrappedSender = connectionWrapperFactory(context, sender)
        queryHandler = queryHandlerFactory(context, wrappedSender, messageFactory)
        registerHandler = registerHandlerFactory(context, wrappedSender, messageFactory)
        optionsHandler = optionsHandlerFactory(context, wrappedSender, messageFactory)
        wrappedSender ! messageFactory.createReadyMessage(stream)
        ready = true
      case OpCodes.Query =>
        if (!ready) {
          initialiseMessageFactory(protocolVersion)
          log.info("Received query before startup message, sending error")
          sender ! Write(messageFactory.createQueryBeforeErrorMessage().serialize())
        } else {
          queryHandler ! QueryHandlerMessages.Query(messageBody, stream)
        }
      case OpCodes.Register =>
        log.debug("Received register message. Sending to RegisterHandler")
        registerHandler ! RegisterHandlerMessages.Register(messageBody, stream)
      case OpCodes.Options =>
        log.debug("Received options message. Sending to OptionsHandler")
        optionsHandler ! OptionsHandlerMessages.OptionsMessage(stream)
      case OpCodes.Prepare =>
        log.debug("Received prepare message. Sending to PrepareHandler")
        val wrappedSender = connectionWrapperFactory(context, sender)
        prepareHandler ! PrepareHandlerMessages.Prepare(messageBody, stream, messageFactory, wrappedSender)
      case OpCodes.Execute =>
        log.debug("Received execute message. Sending to ExecuteHandler")
        val wrappedSender = connectionWrapperFactory(context, sender)
        prepareHandler ! PrepareHandlerMessages.Execute(messageBody, stream, messageFactory, wrappedSender)
      case opCode@_ =>
        log.warning(s"Received unknown opcode $opCode this probably means this feature is yet to be implemented the message body is $messageBody")
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

  /* should not be called if there isn't at least a header */
  private def takeMessage(): Boolean = {
    val protocolVersion = currentData(0)
    if (protocolVersion == VersionThree.clientCode) {
      log.warning("Received a version three message, currently only one and two supported so sending an unsupported protocol error to get the driver to use an older version of the protocol.")
      val wrappedSender = connectionWrapperFactory(context, sender)
      // we can't really send the correct stream back as it is a different type (short rather than byte)
      wrappedSender ! UnsupportedProtocolVersion(0x0)(VersionTwo)
      currentData = ByteString()
      return false
    }


    val stream: Byte = currentData(2)
    val opCode: Byte = currentData(3)

    val bodyLengthArray = currentData.take(ProtocolOneOrTwoHeaderLength).drop(4)
    log.debug(s"Body length array $bodyLengthArray")
    val bodyLength = bodyLengthArray.asByteBuffer.getInt
    log.debug(s"Body length $bodyLength")

    if (currentData.length == bodyLength + ProtocolOneOrTwoHeaderLength) {
      log.debug("Received exactly the whole message")
      partialMessage = false
      val messageBody = currentData.drop(ProtocolOneOrTwoHeaderLength)
      processMessage(opCode, stream, messageBody, protocolVersion)
      currentData = ByteString()
      false
    } else if (currentData.length > (bodyLength + ProtocolOneOrTwoHeaderLength)) {
      partialMessage = true
      log.debug("Received a larger message than the length specifies - assume the rest is another message")
      val messageBody = currentData.drop(ProtocolOneOrTwoHeaderLength).take(bodyLength)
      log.debug(s"Message received ${messageBody.utf8String}")
      processMessage(opCode, stream, messageBody, protocolVersion)
      currentData = currentData.drop(ProtocolOneOrTwoHeaderLength + bodyLength)
      true
    } else {
      log.debug(s"Not received whole message yet, currently ${currentData.length} but need ${bodyLength + 8}")
      partialMessage = true
      dataFromPreviousMessage = currentData
      false
    }
  }
}
