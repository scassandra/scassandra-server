import akka.actor.{ActorSystem}
import akka.testkit.{TestActorRef, TestKit}
import java.net.Socket
import org.scalatest.FunSuiteLike
import org.scalatest.matchers.ShouldMatchers

/**
 * Haven't come up with a way to test interactions with the AKKA TCP manager.
 */
class TcpServerTest extends TestKit(ActorSystem("TestSystem")) with FunSuiteLike with ShouldMatchers {
  test("Should open up a port on startup") {
    TestActorRef(new TcpServer(8042))
    Thread.sleep(2000)

    val socket = new Socket("localhost", 8042)

    socket.isConnected should equal(true)
  }
}
