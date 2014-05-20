package uk.co.scassandra

import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.priming.{PrimingServer}
import uk.co.scassandra.server.TcpServer
import uk.co.scassandra.priming.query.PrimeQueryStore
import uk.co.scassandra.priming.prepared.PrimePreparedStore
import com.typesafe.config.{Config, ConfigFactory}

object ServerStubRunner extends Logging {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val binaryPortNumber = conf.getInt("scassandra.binary.port")
    val adminPortNumber = conf.getInt("scassandra.admin.port")
    logger.info(s"Using binary port to $binaryPortNumber and admin port to $adminPortNumber")
    val ss = new ServerStubRunner(binaryPortNumber, adminPortNumber)
    ss.start()
    ss.awaitTermination()
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
  }

  def awaitTermination() = {
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
    system.awaitTermination()
    logger.info("Server is shut down")
  }

}

