package uk.co.scassandra

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.priming.{PrimingServer}
import uk.co.scassandra.server.TcpServer
import uk.co.scassandra.priming.query.PrimeQueryStore
import uk.co.scassandra.priming.prepared.PrimePreparedStore

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

  val primedResults = PrimeQueryStore()
  val primePreparedStore = new PrimePreparedStore

  def start() = {
    system = ActorSystem("CassandraServerStub")
    system.actorOf(Props(classOf[TcpServer], serverPortNumber, primedResults, primePreparedStore))
    system.actorOf(Props(classOf[PrimingServer], adminPortNumber, primedResults, primePreparedStore))
    system.awaitTermination()
  }

  def awaitStart() = {

  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }

}

