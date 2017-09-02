package org.scassandra.server

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}

package object actors {
  def respondWith(probe: TestProbe, m: Any): Unit = {
    probe.setAutoPilot((sender: ActorRef, msg: Any) => {
      msg match {
        case _ =>
          sender ! m
          TestActor.NoAutoPilot
      }
    })
  }
}
