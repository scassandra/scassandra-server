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
package org.scassandra.server

import akka.actor.{Props, ActorRef, ActorRefFactory, Actor}
import akka.io.Tcp
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.cqlmessages._
import org.scassandra.cqlmessages.response.{ResponseHeader, QueryBeforeReadyMessage, Ready}

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
                        prepareHandler: ActorRef,
                        connectionWrapperFactory: (ActorRefFactory, ActorRef) => ActorRef) extends Actor with Logging {

  import Tcp._

  var ready = false
  var partialMessage = false
  var dataFromPreviousMessage: ByteString = _
  var currentData: ByteString = _
  var messageFactory: CqlMessageFactory = _
  var registerHandler: ActorRef = _
  var queryHandler: ActorRef = _

  val HeaderLength = 8

  def receive = {

    case Received(data: ByteString) => {
      logger.trace(s"Received a message of length ${data.length} data:: $data")

      currentData = data
      if (partialMessage) {
        currentData = dataFromPreviousMessage ++ data
      }

      val messageLength = currentData.length
      logger.trace(s"Whole message length so far is $messageLength")

      while (currentData.length >= HeaderLength && takeMessage()) {}

      if (currentData.length > 0) {
        logger.trace("Not received length yet..")
        partialMessage = true
        dataFromPreviousMessage = currentData
        currentData = ByteString()
      }
    }
    case PeerClosed => {
      logger.info("Client disconnected.")
      context stop self
    }
    case unknown @ _ =>
      logger.warn(s"Unknown message $unknown")

  }

  private def processMessage(opCode: Byte, stream: Byte, messageBody: ByteString, protocolVersion: Byte) = {
    logger.trace(s"Whole body $messageBody with length ${messageBody.length}")

    opCode match {
      case OpCodes.Startup => {
        logger.debug("Sending ready message")
        initialiseMessageFactory(protocolVersion)
        queryHandler = queryHandlerFactory(context, sender, messageFactory)
        registerHandler = registerHandlerFactory(context, sender, messageFactory)
        sender ! Write(messageFactory.createReadyMessage(stream).serialize())
        ready = true
      }
      case OpCodes.Query => {
        if (!ready) {
          initialiseMessageFactory(protocolVersion)
          logger.info("Received query before startup message, sending error")
          sender ! Write(messageFactory.createQueryBeforeErrorMessage().serialize())
        } else {
          queryHandler ! QueryHandlerMessages.Query(messageBody, stream)
        }
      }
      case OpCodes.Register => {
        logger.debug("Received register message. Sending to RegisterHandler")
        registerHandler ! RegisterHandlerMessages.Register(messageBody, stream)
      }
      case OpCodes.Prepare => {
        logger.debug("Received prepare message. Sending to PrepareHandler")
        val wrappedSender = connectionWrapperFactory(context, sender)
        prepareHandler ! PrepareHandlerMessages.Prepare(messageBody, stream, messageFactory, wrappedSender)
      }
      case OpCodes.Execute => {
        logger.debug("Received execute message. Sending to ExecuteHandler")
        val wrappedSender = connectionWrapperFactory(context, sender)
        prepareHandler ! PrepareHandlerMessages.Execute(messageBody, stream, messageFactory, wrappedSender)
      }
      case opCode @ _ =>
        logger.warn(s"Received unknown opcode $opCode this probably means this feature is yet to be implemented the message body is $messageBody")

    }
  }

  def initialiseMessageFactory(protocolVersion: Byte) = {
    messageFactory = if (protocolVersion == ProtocolVersion.ClientProtocolVersionOne) {
      logger.debug("Connection is for protocol version one")
      VersionOneMessageFactory
    } else {
      logger.debug("Connection is for protocol version two")
      VersionTwoMessageFactory
    }
  }

  /* should not be called if there isn't at least a header */
  private def takeMessage(): Boolean = {

    val protocolVersion = currentData(0)
    val stream: Byte = currentData(2)
    val opCode: Byte = currentData(3)

    val bodyLengthArray = currentData.take(HeaderLength).drop(4)
    logger.debug(s"Body length array $bodyLengthArray")
    val bodyLength = bodyLengthArray.asByteBuffer.getInt
    logger.debug(s"Body length $bodyLength")

    if (currentData.length == bodyLength + HeaderLength) {
      logger.debug("Received exactly the whole message")
      partialMessage = false
      val messageBody = currentData.drop(HeaderLength)
      processMessage(opCode, stream, messageBody, protocolVersion)
      currentData = ByteString()
      return false
    } else if (currentData.length > (bodyLength + HeaderLength)) {
      partialMessage = true
      logger.debug("Received a larger message than the length specifies - assume the rest is another message")
      val messageBody = currentData.drop(HeaderLength).take(bodyLength)
      logger.debug(s"Message received ${messageBody.utf8String}")
      processMessage(opCode, stream, messageBody, protocolVersion)
      currentData = currentData.drop(HeaderLength + bodyLength)
      return true
    } else {
      logger.debug(s"Not received whole message yet, currently ${currentData.length} but need ${bodyLength + 8}")
      partialMessage = true
      dataFromPreviousMessage = currentData
      return false
    }
  }
}
