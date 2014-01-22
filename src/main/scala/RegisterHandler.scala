import akka.actor.{ActorRef, Actor}
import akka.io.Tcp.Write
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging

class RegisterHandler(connection: ActorRef) extends Actor with Logging {
  def receive = {
    case registerMsg @ RegisterHandlerMessages.Register(_) => {
      logger.info(s"Received register message ${registerMsg}")
      connection ! Write(Ready().serialize())
    }
    case msg @ _ => {
      logger.warn(s"Received unknown message ${msg}")
    }
  }
}

object RegisterHandlerMessages {
  case class Register(messageBody: ByteString)

}
