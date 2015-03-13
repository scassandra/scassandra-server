package org.scassandra.server.actors

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe, TestKit}
import org.scalatest.{FunSuiteLike, Matchers}
import org.scassandra.server.cqlmessages.VersionTwoMessageFactory

class OptionsHandlerTest extends TestKit(ActorSystem("TestSystem")) with FunSuiteLike with Matchers {

  test("Should send supported message on any Options message") {
    val senderTestProbe = TestProbe()
    val cqlMessageFactory = VersionTwoMessageFactory
    val stream : Byte = 0x24

    val expectedSupportedMessage = cqlMessageFactory.createSupportedMessage(stream)
    val underTest = TestActorRef(new OptionsHandler(senderTestProbe.ref, cqlMessageFactory))

    underTest ! OptionsHandlerMessages.OptionsMessage(stream)

    senderTestProbe.expectMsg(expectedSupportedMessage)
  }

}
