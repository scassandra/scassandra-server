package org.scassandra.server.actors.priming

import akka.actor.Props
import org.scalatest.{ Matchers, WordSpec }
import org.scassandra.server.actors.TestKitWithShutdown
import org.scassandra.server.priming.prepared.{ PreparedStore, PrimePreparedSingle, PrimePreparedStore }
import scala.reflect.runtime.universe._

class PrimePreparedStoreActorTest extends WordSpec with TestKitWithShutdown with Matchers {

  "prime prepared store" must {
    "construct with a real prepared store" in {
      val store: PreparedStore[PrimePreparedSingle] = new PrimePreparedStore()
      val underTest = system.actorOf(Props(classOf[PrimePreparedStoreActor[PrimePreparedSingle]], store, typeTag[PrimePreparedSingle]))
    }
  }
}
