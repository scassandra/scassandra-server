package org.scassandra.server.actors

import akka.actor.Actor
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.messages.BatchQueryKind.BatchQueryKind
import org.scassandra.codec.messages.BatchType
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.server.actors.Activity.BatchExecution
import org.scassandra.server.actors.PrimeBatchStoreActor._
import org.scassandra.server.priming.Defaulter
import org.scassandra.server.priming.query.{Prime, Then}
import org.scassandra.server.priming.routes.PrimingJsonHelper.extractPrime

class PrimeBatchStoreActor extends Actor {

  import context._

  def receive: Receive = currentPrimes(Map())

  def currentPrimes(primes: Map[BatchCriteria, BatchPrimeSingle]): Receive = {
    case MatchBatch(primeMatch) =>
      val prime = primes.find {
        case (batchCriteria, _) =>
          batchCriteria.queries == primeMatch.batchQueries.map(bq => BatchQueryPrime(bq.query, bq.batchQueryKind)) &&
            batchCriteria.consistency.contains(primeMatch.consistency) &&
            batchCriteria.batchType == primeMatch.batchType
      } map (_._2.prime)

      sender() ! MatchResult(prime)

    case RecordBatchPrime(prime) =>
      val p: BatchPrimeSingle = prime.withDefaults
      val criteria = BatchCriteria(p.when.queries, p.when.consistency.get, p.when.batchType.get)
      become(currentPrimes(primes + (criteria -> p)))

    case ClearPrimes =>
      become(currentPrimes(Map()))
  }
}

object PrimeBatchStoreActor {
  case class RecordBatchPrime(prime: BatchPrimeSingle)
  case class MatchBatch(batch: BatchExecution)
  case class MatchResult(result: Option[Prime])

  case object ClearPrimes

  case class BatchCriteria(queries: Seq[BatchQueryPrime], consistency: List[Consistency], batchType: BatchType)

  case class BatchPrimeSingle(when: BatchWhen, thenDo: Then) {
    @transient lazy val prime = {
      extractPrime(thenDo)
    }

    def withDefaults: BatchPrimeSingle =
    // join queries into 1 string so '?' can be counted.
      copy(when.withDefaults, thenDo.withDefaults(Some(when.queries.map(_.text).mkString("\n"))))
  }

  case class BatchWhen(queries: List[BatchQueryPrime], consistency: Option[List[Consistency]] = None, batchType: Option[BatchType] = None) {
    def withDefaults: BatchWhen = copy(batchType = batchType.orElse(Some(BatchType.LOGGED)), consistency = Defaulter.defaultConsistency(consistency))
  }

  case class BatchQueryPrime(text: String, kind: BatchQueryKind)
}
