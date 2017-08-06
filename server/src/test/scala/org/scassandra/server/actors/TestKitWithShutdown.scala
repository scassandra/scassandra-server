package org.scassandra.server.actors

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestKitBase}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestKitWithShutdown extends TestKitBase with BeforeAndAfterAll {
  self: Suite =>

  lazy implicit val system = ActorSystem(this.getClass.getCanonicalName.replace(".", "-"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}
