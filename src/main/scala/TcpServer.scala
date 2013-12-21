import akka.actor.{Props, Actor}
import akka.io.{IO, Tcp}
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

object HeaderConsts {
  val Length = 4
}
