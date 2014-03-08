package uk.co.scassandra.server

import akka.actor.{ActorRef, ActorRefFactory, Props, Actor}
import akka.io.Tcp.PeerClosed
import akka.io.{IO, Tcp}
import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress

class TcpServer(port: Int) extends Actor with Logging {

  import Tcp._
  import context.system

  val manager = IO(Tcp)

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))

  def receive = {
    case b@Bound(localAddress) =>
      logger.info(s"Server bound to port $port")

    case CommandFailed(_: Bind) =>
      context stop self

    case c@Connected(remote, local) =>
      logger.info("Incoming connection, creating a connection handler!")
      val handler = context.actorOf(Props(classOf[ConnectionHandler],
        (af: ActorRefFactory, sender: ActorRef) => af.actorOf(Props(classOf[QueryHandler], sender)),
        (af: ActorRefFactory, sender: ActorRef) => af.actorOf(Props(classOf[RegisterHandler], sender))
      ))
      logger.info(s"Sending register with connection handler $handler")
      sender ! Register(handler)

  }
}
