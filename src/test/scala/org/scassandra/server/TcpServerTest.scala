package org.scassandra.server

import org.scassandra.priming.{ActivityLog}
import org.scalatest.{BeforeAndAfter, FunSuiteLike, Matchers}
import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import akka.actor.ActorSystem
import akka.io.Tcp.Connected
import org.scassandra.priming.query.PrimeQueryStore
import org.scassandra.priming.prepared.PrimePreparedStore

/**
 * Unfortunately this test actually binds to the port. Not found a way to
 * stub out the akka IO manager.
 */
class TcpServerTest extends TestKit(ActorSystem("Test")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  test("Should record a connection with the ActivityLog") {
    //given
    ActivityLog.clearConnections()
    val underTest = TestActorRef(new TcpServer(8044, new PrimeQueryStore, new PrimePreparedStore))
    //when
    underTest ! Connected(null, null)
    //then
    ActivityLog.retrieveConnections().size should equal(1)
  }
}
