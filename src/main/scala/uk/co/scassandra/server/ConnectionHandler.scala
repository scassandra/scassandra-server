package uk.co.scassandra.server

import akka.actor.{Props, ActorRef, ActorRefFactory, Actor}
import akka.io.Tcp
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.cqlmessages.{ProtocolVersion, OpCodes}
import uk.co.scassandra.cqlmessages.response.{ResponseHeader, QueryBeforeReadyMessage, Ready}
import uk.co.scassandra.cqlmessages.response.{VersionOneMessageFactory, CqlMessageFactory, VersionTwoMessageFactory}

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

  val HeaderLength = 8

  def receive = {

    case Received(data: ByteString) =>
      logger.debug(s"Received a message of length ${data.length} data:: $data")

      currentData = data
      if (partialMessage) {
        currentData = dataFromPreviousMessage ++ data
      }

      val messageLength = currentData.length
      logger.debug(s"Whole message length so far is $messageLength")

      // TODO - [DN] is this code blocking?
      // [CB] How so? It works until there is nothing else to do. There are no sleeps.
      while (currentData.length >= HeaderLength && takeMessage()) {}

      if (currentData.length > 0) {
        logger.debug("Not received length yet..")
        partialMessage = true
        dataFromPreviousMessage = currentData
        currentData = ByteString()
      }


    case PeerClosed => context stop self
    case unknown@_ =>
      logger.warn(s"Unknown message $unknown")

  }

  private def processMessage(opCode: Byte, stream: Byte, messageBody: ByteString, protocolVersion: Byte) = {
    logger.debug(s"Whole body $messageBody with length ${messageBody.length}")

    val cqlMessageFactory = if (protocolVersion == ProtocolVersion.ClientProtocolVersionOne) {
      logger.debug("Received protocol one message")
      VersionOneMessageFactory
    } else {
      logger.debug("Received protocol two message")
      VersionTwoMessageFactory
    }

    opCode match {
      case OpCodes.Startup => {
        logger.info("Sending ready message")
        sender ! Write(cqlMessageFactory.createReadyMessage(stream).serialize())
        ready = true
      }
      case OpCodes.Query => {
        if (!ready) {
          logger.info("Received query before startup message, sending error")
          sender ! Write(cqlMessageFactory.createQueryBeforeErrorMessage().serialize())
        } else {
          val queryHandler = queryHandlerFactory(context, sender, cqlMessageFactory)
          queryHandler ! QueryHandlerMessages.Query(messageBody, stream)
        }
      }
      case OpCodes.Register => {
        logger.debug("Received register message. Sending to RegisterHandler")
        val registerHandler = registerHandlerFactory(context, sender, cqlMessageFactory)
        registerHandler ! RegisterHandlerMessages.Register(messageBody)
      }
      case OpCodes.Prepare => {
        logger.debug("Received prepare message. Sending to PrepareHandler")
//        val wrappedSender = context.actorOf(Props(classOf[TcpConnectionWrapper], sender))
        val wrappedSender = connectionWrapperFactory(context, sender)
        prepareHandler ! PrepareHandlerMessages.Prepare(messageBody, stream, cqlMessageFactory, wrappedSender)
      }
      case OpCodes.Execute => {
        logger.debug("Received execute message. Sending to ExecuteHandler")
        //val wrappedSender = context.actorOf(Props(classOf[TcpConnectionWrapper], sender))
        val wrappedSender = connectionWrapperFactory(context, sender)
        prepareHandler ! PrepareHandlerMessages.Execute(messageBody, stream, cqlMessageFactory, wrappedSender)
      }
      case opCode @ _ =>
        logger.warn(s"Received unknown opcode $opCode this probably means this feature is yet to be implemented the message body is $messageBody")

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
