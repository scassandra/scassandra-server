package uk.co.scassandra.server

import akka.actor.{ActorRef, ActorRefFactory, Actor}
import akka.io.Tcp
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging
import com.batey.narinc.client.cqlmessages.{ResponseHeader, Ready, QueryBeforeReadyMessage, OpCodes}

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
class ConnectionHandler(queryHandlerFactory: (ActorRefFactory, ActorRef) => ActorRef,
                        registerHandlerFactory: (ActorRefFactory, ActorRef) => ActorRef) extends Actor with Logging {

  import Tcp._

  var ready = false
  var partialMessage = false
  var dataFromPreviousMessage: ByteString = _
  var currentData: ByteString = _

  val HeaderLength = 8

  def receive = {

    case Received(data: ByteString) =>
      logger.info(s"Received a message of length ${data.length} data:: $data")

      currentData = data
      if (partialMessage) {
        currentData = dataFromPreviousMessage ++ data
      }

      val messageLength = currentData.length
      logger.info(s"Whole message length so far is $messageLength")

      // TODO - [DN] is this code blocking?
      // [CB] How so? It works until there is nothing else to do. There are no sleeps.
      while (currentData.length >= HeaderLength && takeMessage()) {}

      if (currentData.length > 0) {
        logger.info("Not received length yet..")
        partialMessage = true
        dataFromPreviousMessage = currentData
        currentData = ByteString()
      }


    case PeerClosed => context stop self
    case unknown@_ =>
      logger.info(s"Unknown message $unknown")

  }

  private def processMessage(opCode: Byte, stream: Byte, messageBody: ByteString) = {
    logger.info(s"Whole body $messageBody with length ${messageBody.length}")

    opCode match {
      case OpCodes.Startup =>
        logger.info("Sending ready message")
        sender ! Write(Ready(stream).serialize())
        ready = true

      case OpCodes.Query =>
        if (!ready) {
          logger.info("Received query before startup message, sending error")
          sender ! Write(QueryBeforeReadyMessage(ResponseHeader.DefaultStreamId).serialize())
        } else {
          val queryHandler = queryHandlerFactory(context, sender)
          queryHandler ! QueryHandlerMessages.Query(messageBody, stream)
        }

      case OpCodes.Register =>
        logger.info("Received register message. Sending to RegisterHandler")
        val registerHandler = registerHandlerFactory(context, sender)
        registerHandler ! RegisterHandlerMessages.Register(messageBody)

      case opCode@_ =>
        logger.info(s"Received unknown opcode $opCode")

    }
  }

  /* should not be called if there isn't at least a header */
  private def takeMessage(): Boolean = {

    val opCode: Byte = currentData(3)
    val stream: Byte = currentData(2)

    val bodyLengthArray = currentData.take(HeaderLength).drop(4)
    logger.info(s"Body length array $bodyLengthArray")
    val bodyLength = bodyLengthArray.asByteBuffer.getInt
    logger.info(s"Body length $bodyLength")

    if (currentData.length == bodyLength + HeaderLength) {
      logger.info("Received exactly the whole message")
      partialMessage = false
      val messageBody = currentData.drop(HeaderLength)
      processMessage(opCode, stream, messageBody)
      currentData = ByteString()
      return false
    } else if (currentData.length > (bodyLength + HeaderLength)) {
      partialMessage = true
      logger.info("Received a larger message than the length specifies - assume the rest is another message")
      val messageBody = currentData.drop(HeaderLength).take(bodyLength)
      logger.info(s"Message received ${messageBody.utf8String}")
      processMessage(opCode, stream, messageBody)
      currentData = currentData.drop(HeaderLength + bodyLength)
      return true
    } else {
      logger.info(s"Not received whole message yet, currently ${currentData.length} but need ${bodyLength + 8}")
      partialMessage = true
      dataFromPreviousMessage = currentData
      return false
    }
  }
}
