import akka.actor.{ActorSystem, Props}
import com.typesafe.scalalogging.slf4j.Logging

object ServerStubRunner extends Logging {

  var portNumber = 8042
  var system : ActorSystem = _

  def main(args: Array[String]) {
    if (args.length > 0) {
      val port = args(0)
      logger.info(s"Overriding port to ${port}")
      portNumber = port.toInt
    }
    run()
  }

  def run() = {
    system = ActorSystem()
    system.actorOf(Props(classOf[TcpServer], portNumber))
    system.awaitTermination()
  }

  def shutdown() = {
    system.shutdown()
  }

}

