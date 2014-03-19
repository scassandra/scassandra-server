package uk.co.scassandra.server

import akka.actor.{ActorRef, ActorRefFactory, Props, Actor}
import akka.io.{IO, Tcp}
import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress
import uk.co.scassandra.priming.{ActivityLog, PrimedResults}

class TcpServer(port: Int, primedResults: PrimedResults) extends Actor with Logging {

  import Tcp._
  import context.system

  val manager = IO(Tcp)

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  def receive = {
    case b @ Bound(localAddress) =>
      logger.info(s"Server bound to port $port")

    case CommandFailed(_: Bind) =>
      context stop self

    case c @ Connected(remote, local) =>
      logger.info(s"Incoming connection, creating a connection handler! ${remote} ${local}")
      ActivityLog.recordConnection()
      val handler = context.actorOf(Props(classOf[ConnectionHandler],
        (af: ActorRefFactory, sender: ActorRef) => af.actorOf(Props(classOf[QueryHandler], sender, primedResults)),
        (af: ActorRefFactory, sender: ActorRef) => af.actorOf(Props(classOf[RegisterHandler], sender))
      ))
      logger.info(s"Sending register with connection handler $handler")
      sender ! Register(handler)

  }
}
