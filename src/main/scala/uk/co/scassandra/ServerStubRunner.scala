package uk.co.scassandra

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.priming.{PrimingServer, PrimedResults}
import uk.co.scassandra.server.TcpServer

object ServerStubRunner extends Logging {
  def main(args: Array[String]) {
    var binaryPortNumber = 8042
    var adminPortNumber = 8043
    if (args.length == 2) {
      val binaryPort = args(0)
      val adminPort = args(1)
      logger.info(s"Overriding binary port to ${binaryPort} and admin port to $adminPort")
      binaryPortNumber = binaryPort.toInt
      adminPortNumber = adminPort.toInt
    }
    new ServerStubRunner(binaryPortNumber, adminPortNumber).start()
  }
}

class ServerStubRunner(val serverPortNumber: Int = 8042, val adminPortNumber : Int = 8043) extends Logging {

  var system : ActorSystem = _

  val primedResults = PrimedResults()

  def start() = {
    system = ActorSystem("CassandraServerStub")
    system.actorOf(Props(classOf[TcpServer], serverPortNumber, primedResults))
    system.actorOf(Props(classOf[PrimingServer], adminPortNumber, primedResults))
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }

}

