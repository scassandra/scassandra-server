import akka.actor.Actor
import akka.io.Tcp
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging

class ConnectionHandler extends Actor with Logging {
  import Tcp._

  var ready = false

  def receive = {
    case Received(data : ByteString) =>  {
      logger.info(s"Received a message ${data}")
      val header = data.take(HeaderConsts.Length)
      logger.info(s"Header ${header}")
      val length = data(7)
      logger.info(s"Length is ${length}")

      header(3) match {
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
            logger.info("Sending result")
            // TODO: Parse the query and see if it is a use statement
            sender ! Write(VoidResult.serialize())
          }
        }
        case opCode @ _ => {
          logger.info(s"Received unknown opcode ${opCode}")
        }
      }
    }
    case PeerClosed => context stop self
  }
}

object HeaderConsts {
  val Length = 4
}