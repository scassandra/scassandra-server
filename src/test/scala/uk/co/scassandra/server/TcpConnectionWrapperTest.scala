package uk.co.scassandra.server

import akka.testkit.{TestProbe, TestActorRef, TestKit}
import akka.actor.ActorSystem
import org.scalatest.FunSuiteLike
import uk.co.scassandra.cqlmessages.response.VoidResult
import akka.io.Tcp.Write
import uk.co.scassandra.cqlmessages.VersionTwo

class TcpConnectionWrapperTest extends TestKit(ActorSystem("TestSystem")) with FunSuiteLike  {

  implicit val impProtocolVersion = VersionTwo

  test("Should forward serialised response message") {
    val testProbeForTcpConnection = TestProbe()
    val underTest = TestActorRef(new TcpConnectionWrapper(testProbeForTcpConnection.ref))
    val anyResponse: VoidResult = VoidResult(0x0.toByte)

    underTest ! anyResponse

    testProbeForTcpConnection.expectMsg(Write(anyResponse.serialize()))
  }
}
