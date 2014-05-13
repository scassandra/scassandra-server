package uk.co.scassandra.server

import akka.actor.{ActorRef, ActorRefFactory, Props, Actor}
import akka.io.{IO, Tcp}
import com.typesafe.scalalogging.slf4j.Logging
import java.net.InetSocketAddress
import uk.co.scassandra.priming.{ActivityLog}
import uk.co.scassandra.priming.query.PrimeQueryStore
import uk.co.scassandra.priming.prepared.PrimePreparedStore
import uk.co.scassandra.cqlmessages.CqlMessageFactory

class TcpServer(port: Int, primedResults: PrimeQueryStore, primePrepareStore: PrimePreparedStore) extends Actor with Logging {

  import Tcp._
  import context.system

  val manager = IO(Tcp)

  IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", port))
  val preparedHandler = context.actorOf(Props(classOf[PrepareHandler], primePrepareStore))

  def receive = {
    case b @ Bound(localAddress) =>
      logger.info(s"Port $port ready for Cassandra binary connections.")

    case CommandFailed(_: Bind) =>
      context stop self

    case c @ Connected(remote, local) =>
      logger.debug(s"Incoming connection, creating a connection handler! $remote $local")
      ActivityLog.recordConnection()
      val handler = context.actorOf(Props(classOf[ConnectionHandler],
        (af: ActorRefFactory, sender: ActorRef, msgFactory: CqlMessageFactory) => af.actorOf(Props(classOf[QueryHandler], sender, primedResults, msgFactory)),
        (af: ActorRefFactory, sender: ActorRef, msgFactory: CqlMessageFactory) => af.actorOf(Props(classOf[RegisterHandler], sender, msgFactory)),
        preparedHandler,
        (af: ActorRefFactory, tcpConnection: ActorRef) => af.actorOf(Props(classOf[TcpConnectionWrapper], tcpConnection)))
      )
      logger.debug(s"Sending register with connection handler $handler")
      sender ! Register(handler)
  }
}
