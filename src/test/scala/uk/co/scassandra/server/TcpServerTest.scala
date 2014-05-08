package uk.co.scassandra.server

import uk.co.scassandra.priming.{ActivityLog}
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Matchers}
import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import akka.actor.ActorSystem
import akka.io.Tcp.Connected
import uk.co.scassandra.priming.query.PrimeQueryStore

/**
 * Unfortunately this test actually binds to the port. Not found a way to
 * stub out the akka IO manager.
 */
class TcpServerTest extends TestKit(ActorSystem("Test")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  test("Should record a connection with the ActivityLog") {
    //given
    ActivityLog.clearConnections()
    val underTest = TestActorRef(new TcpServer(8044, new PrimeQueryStore))
    //when
    underTest ! Connected(null, null)
    //then
    ActivityLog.retrieveConnections().size should equal(1)
  }
}
