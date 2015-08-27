package org.scassandra.server.priming.batch

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.cqlmessages.{LOGGED, BatchType, Consistency}
import org.scassandra.server.priming.{BatchExecution, PrimeResult}
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.routes.PrimeQueryResultExtractor

class PrimeBatchStore extends LazyLogging {

  var primes: Map[BatchCriteria, BatchPrime] = Map()
  
  def record(prime: BatchPrimeSingle): Unit = {
    val result: PrimeResult = PrimeQueryResultExtractor.convertToPrimeResult(Map(), prime.thenDo.result.getOrElse(Success))
    val consistencies: List[Consistency] = prime.when.consistency.getOrElse(Consistency.all)
    primes += (BatchCriteria(prime.when.queries, consistencies, prime.when.batchType.getOrElse(LOGGED)) -> BatchPrime(result))
  }
  def findPrime(primeMatch: BatchExecution): Option[BatchPrime] = {
    logger.debug("Batch Prime Match {} current primes {}", primeMatch, primes)
    primes.find {
      case (batchCriteria, _) =>
        batchCriteria.queries == primeMatch.batchQueries.map(bq => BatchQueryPrime(bq.query, bq.batchQueryKind)) &&
          batchCriteria.consistency.contains(primeMatch.consistency) &&
          batchCriteria.batchType == primeMatch.batchType
    } map(_._2)
  }
}

case class BatchCriteria(queries: Seq[BatchQueryPrime], consistency: List[Consistency], batchType: BatchType)
// batches can only be DML statements
// todo delay
case class BatchPrime(result: PrimeResult)
