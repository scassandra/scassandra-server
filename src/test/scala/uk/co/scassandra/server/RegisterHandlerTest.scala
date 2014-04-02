package uk.co.scassandra.server

import akka.actor.ActorSystem
import akka.io.Tcp.Write
import akka.testkit.{TestActorRef, TestProbe, TestKit}
import akka.util.ByteString
import org.scalatest.FunSuiteLike
import org.scalatest.matchers.ShouldMatchers
import org.scassandra.cqlmessages.response.Ready

class RegisterHandlerTest extends TestKit(ActorSystem("TestSystem")) with FunSuiteLike with ShouldMatchers {
  test("Should send Ready message on any Register message") {
    val senderTestProbe = TestProbe()
    val underTest = TestActorRef(new RegisterHandler(senderTestProbe.ref))
    val registerBody = MessageHelper.createRegisterMessageBody()

    underTest ! RegisterHandlerMessages.Register(ByteString(registerBody.toArray))

    senderTestProbe.expectMsg(Write(Ready().serialize()))
  }
}
