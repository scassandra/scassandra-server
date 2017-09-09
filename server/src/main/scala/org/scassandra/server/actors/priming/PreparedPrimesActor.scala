package org.scassandra.server.actors.priming

import akka.actor.{ Actor, ActorRef }
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import cats.implicits._
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.PrimeMatch

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Wraps access to all priming for prepared statements
 *
 * @param primeStores Order of priority prime stores to forward to
 */
class PreparedPrimesActor(primeStores: List[ActorRef]) extends Actor {

  import context._

  private implicit val timeout: Timeout = Timeout(1 second)

  def receive = {
    case msg =>
      val primeResponse: Future[PrimeMatch] = primeStores
        .map(store => (store ? msg).mapTo[PrimeMatch])
        .sequence[Future, PrimeMatch]
        .map(l => {
          l.filter(_.prime.isDefined) match {
            case Nil => PrimeMatch(None)
            case x => x.head
          }
        })
      primeResponse pipeTo sender
  }
}

