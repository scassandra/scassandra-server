import akka.actor.{ActorRef, ActorRefFactory, Actor}
import akka.io.Tcp
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging

class ConnectionHandler(queryHandlerMaker: (ActorRefFactory, ActorRef) => ActorRef) extends Actor with Logging {
  import Tcp._

  var ready = false
  var partialMessage = false
  var previousPart : ByteString = _

  def receive = {

    case Received(data : ByteString) =>  {
      logger.info(s"Received a message ${data}")

      var allData = data
      if (partialMessage) {
        allData = previousPart ++ data
      }
      val messageLength = allData.length

      logger.info(s"Whole message length so far is ${messageLength}")

      if (messageLength >= 8) {
        val opCode : Byte = allData(3)
        logger.info(s"Length is ${messageLength}")

        val bodyLengthArray  = allData.take(8).drop(4)
        logger.info(s"Body length buffer ${bodyLengthArray}")

        val bodyLength = bodyLengthArray.asByteBuffer.getInt
        logger.info(s"Body length ${bodyLength}")

        if (allData.length == bodyLength + 8) {
          partialMessage = false
          val messageBody = allData.drop(8).take(bodyLength)
          logger.info(s"Whole body ${messageBody}")

          opCode match {
            case OpCodes.Startup => {
              logger.info("Sending ready message")
              sender ! Write(Ready.serialize())
              ready = true
            }
            case OpCodes.Query => {
              if (!ready) {
                logger.info("Received query before startup message, sending error")
                sender ! Write(QueryBeforeReadyMessage.serialize())
              } else {
                val queryHandler = queryHandlerMaker(context, sender)
                queryHandler ! QueryHandlerMessages.Query(messageBody)
              }
            }
            case opCode @ _ => {
              logger.info(s"Received unknown opcode ${opCode}")
            }
          }
        } else {
          logger.info(s"Not received whole message yet, currently ${allData.length} but need ${bodyLength + 8}")
          partialMessage = true
          previousPart = allData
        }

      } else {
        logger.info("Not received length yet..")
        partialMessage = true
        previousPart = allData
      }
    }
    case PeerClosed => context stop self
    case unknown @ _ => {
      logger.info(s"Unknown message ${unknown}")
    }
  }
}
