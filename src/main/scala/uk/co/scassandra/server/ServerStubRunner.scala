package uk.co.scassandra.server

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging
import priming.{PrimedResults, PrimingServer}

object ServerStubRunner extends Logging {

  var portNumber = 8042
  var primingPortNumber = 8043
  var system : ActorSystem = _

  val primedResults = PrimedResults()

  def main(args: Array[String]) {
    if (args.length > 0) {
      val port = args(0)
      logger.info(s"Overriding port to $port")
      portNumber = port.toInt
    }
    run()
  }

  def run() = {
    system = ActorSystem("CassandraServerStub")
    system.actorOf(Props(classOf[TcpServer], portNumber))
    system.actorOf(Props(classOf[PrimingServer], primingPortNumber, primedResults))
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }

}

