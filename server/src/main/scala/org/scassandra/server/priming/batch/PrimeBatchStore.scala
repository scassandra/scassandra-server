package org.scassandra.server.priming.batch

import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.priming.{BatchExecution, PrimeResult}
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.routes.PrimeQueryResultExtractor

class PrimeBatchStore {
  
  var primes: Map[BatchCriteria, BatchPrime] = Map()
  
  def record(prime: BatchPrimeSingle): Unit = {
    val result: PrimeResult = PrimeQueryResultExtractor.convertToPrimeResult(Map(), prime.thenDo.result.getOrElse(Success))
    val consistencies: List[Consistency] = prime.when.consistency.getOrElse(Consistency.all)
    primes += (BatchCriteria(prime.when.queries, consistencies) -> BatchPrime(result))
  }
  def findPrime(primeMatch: BatchExecution): Option[BatchPrime] = {
    primes.find {
      case (batchCriteria, _) =>
        batchCriteria.queries == primeMatch.batchQueries.map(bq => BatchQueryPrime(bq.query, bq.batchQueryKind)) &&
          batchCriteria.consistency.contains(primeMatch.consistency)
    } map(_._2)
  }
}

case class BatchCriteria(queries: Seq[BatchQueryPrime], consistency: List[Consistency])
// batches can only be DML statements
// todo delay
case class BatchPrime(result: PrimeResult)
