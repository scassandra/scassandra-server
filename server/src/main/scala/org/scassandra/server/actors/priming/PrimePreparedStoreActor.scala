package org.scassandra.server.actors.priming

import akka.Done
import akka.actor.Actor
import org.scassandra.codec.{ Execute, Prepare, ProtocolVersion }
import org.scassandra.server.actors.priming.PrimePreparedStoreActor._
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{ ClearQueryPrimes, Prime }
import org.scassandra.server.priming.prepared.{ PreparedPrimeIncoming, PreparedStore }

import scala.reflect.runtime.universe._

/**
 * Thin wrapper around the legacy way we did prime stores
 * todo: test
 */
class PrimePreparedStoreActor[T <: PreparedPrimeIncoming: TypeTag](store: PreparedStore[T]) extends Actor {
  def receive: Receive = {
    case RecordPSPrime(prime: T) =>
      sender() ! store.record(prime)
    case ClearPSPrime =>
      store.clear()
      sender ! Done
    case GetAllPSPrimes =>
      sender() ! AllPSPrimes(store.retrievePrimes())
    case LookupByPrepare(prepare, id) =>
      sender() ! PrimeMatch(store.apply(prepare, id))
    case LookupByExecute(queryText, execute, version) =>
      sender() ! PrimeMatch(store.apply(queryText, execute)(version))
  }
}

object PrimePreparedStoreActor {
  case class RecordPSPrime[T](prime: T)

  case class LookupByPrepare(prepare: Prepare, idToUse: Int)
  case class LookupByExecute(queryText: String, execute: Execute, version: ProtocolVersion)
  case class PrimeMatch(prime: Option[Prime])

  case object ClearPSPrime

  case object GetAllPSPrimes
  case class AllPSPrimes[T](primes: List[T])
}