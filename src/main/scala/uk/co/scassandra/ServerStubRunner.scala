package uk.co.scassandra

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.priming.{PrimingServer, PrimedResults}
import uk.co.scassandra.server.TcpServer

object ServerStubRunner extends Logging {
  def main(args: Array[String]) {
    var portNumber = 8042
    if (args.length > 0) {
      val port = args(0)
      logger.info(s"Overriding port to $port")
      portNumber = port.toInt
    }
    new ServerStubRunner(portNumber).start()
  }
}

class ServerStubRunner(val serverPortNumber: Int = 8042, val primingPortNumber : Int = 8043) extends Logging {

  var system : ActorSystem = _

  val primedResults = PrimedResults()

  def start() = {
    system = ActorSystem("CassandraServerStub")
    system.actorOf(Props(classOf[TcpServer], serverPortNumber, primedResults))
    system.actorOf(Props(classOf[PrimingServer], primingPortNumber, primedResults))
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }

}

