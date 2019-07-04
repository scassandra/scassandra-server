/*
 * Copyright (C) 2017 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  def receive: Receive = {
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

