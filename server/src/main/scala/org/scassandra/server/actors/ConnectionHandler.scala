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

import akka.actor._
import akka.util.ByteString
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.response.UnsupportedProtocolVersion

/*
 * Deals with partial messages and multiple messages coming in the same Received
 */
class ConnectionHandler(queryHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        batchHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        registerHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        optionsHandlerFactory: (ActorRefFactory, ActorRef, CqlMessageFactory) => ActorRef,
                        prepareHandler: ActorRef,
                        connectionWrapperFactory: (ActorRefFactory, ActorRef) => ActorRef) extends Actor with ActorLogging {

  import akka.io.Tcp._

  var ready = false
  var partialMessage = false
  var dataFromPreviousMessage: ByteString = _
  var currentData: ByteString = _
  var registerHandler: ActorRef = _
  val ProtocolOneOrTwoHeaderLength = 8

  // Extracted to handle full messages
  val cqlMessageHandler = context.actorOf(Props(classOf[NativeProtocolMessageHandler],
    queryHandlerFactory,
    batchHandlerFactory,
    registerHandlerFactory,
    optionsHandlerFactory,
    prepareHandler,
    connectionWrapperFactory
  ))

  def receive = {
    case Received(data: ByteString) =>
      currentData = data
      if (partialMessage) {
        currentData = dataFromPreviousMessage ++ data
      }

      // the header could be 8 or 9 bits now :(
      while (currentData.length >= ProtocolOneOrTwoHeaderLength && takeMessage()) {}

      if (currentData.nonEmpty) {
        partialMessage = true
        dataFromPreviousMessage = currentData
        currentData = ByteString()
      }
    case PeerClosed =>
      log.info("Client disconnected.")
      context stop self
  }

  /* should not be called if there isn't at least a header */
  private def takeMessage(): Boolean = {
    val protocolVersion = currentData(0)
    if (protocolVersion >= VersionThree.clientCode) {
      log.warning(s"Received protocol version $protocolVersion, currently only one and two supported so sending an unsupported protocol error to get the driver to use an older version of the protocol.")
      val wrappedSender = connectionWrapperFactory(context, sender())
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
      cqlMessageHandler forward NativeProtocolMessageHandler.Process(opCode, stream, messageBody, protocolVersion)
      currentData = ByteString()
      false
    } else if (currentData.length > (bodyLength + ProtocolOneOrTwoHeaderLength)) {
      partialMessage = true
      log.debug("Received a larger message than the length specifies - assume the rest is another message")
      val messageBody = currentData.drop(ProtocolOneOrTwoHeaderLength).take(bodyLength)
      log.debug(s"Message received ${messageBody.utf8String}")
      cqlMessageHandler forward NativeProtocolMessageHandler.Process(opCode, stream, messageBody, protocolVersion)
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
