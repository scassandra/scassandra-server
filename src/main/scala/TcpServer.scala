import akka.actor.{Props, Actor}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress

class TcpServer(port: Int) extends Actor with Logging {
  import Tcp._
  import context.system
  val manager = IO(Tcp)

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  def receive = {
    case b @ Bound(localAddress) => {
      logger.info(s"Server bound to port ${port}..")
    }

    case CommandFailed(_: Bind) => context stop self

    case c @ Connected(remote, local) =>
      logger.info("Incoming connection, creating a connection handler!")
      val handler = context.actorOf(Props[ConnectionHandler])
      val connection = sender
      connection ! Register(handler)
  }
}

class ConnectionHandler extends Actor with Logging {
  import Tcp._
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
        }
        case OpCodes.Query => {
          logger.info("Sending result")
          // TODO: Parse the query and see if it is a use statement
          sender ! Write(VoidResult.serialize())
        }
        case opCode @ _ => {
          logger.info(s"Received unknown opcode ${opCode}")
        }
      }
    }
    case PeerClosed => context stop self
  }

  def convertToByteString(input : List[Int]) : ByteString = {
    val bs = ByteString.newBuilder
    for (byte <- input) {
      bs.putByte(byte.toByte)
    }
    bs.result()
  }
}

object HeaderConsts {
  val Length = 4
}
