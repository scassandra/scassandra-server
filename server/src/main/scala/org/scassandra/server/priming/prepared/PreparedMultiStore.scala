package org.scassandra.server.priming.prepared

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, PrimeMatch}
import org.scassandra.server.priming.routes.PrimeQueryResultExtractor

class PreparedMultiStore extends PreparedStoreLookup with LazyLogging {

  var state: Map[PrimeCriteria, PreparedPrimeResult] = Map()

  // todo change to a result to include validation failures etc
  def record(prime: PrimePreparedMulti): Unit = {
    val variableTypes = prime.thenDo.variable_types.getOrElse(List())

    val outcomes: List[(List[Option[Any]], Prime)] = prime.thenDo.outcomes.map(o => {
      val result = PrimeQueryResultExtractor.convertToPrimeResult(Map(), o.action.result.getOrElse(Success))
      (o.criteria.variable_matcher.map(_.matcher), Prime(result = result))
    })

    val finalPrime = PreparedMultiPrime(variableTypes, outcomes.toMap)
    val criteria: PrimeCriteria = PrimeCriteria(prime.when.query.get, List())
    logger.info("Storing prime {} for with criteria {}", finalPrime, criteria)
    state += (criteria -> finalPrime)
  }

  def findPrime(primeMatch: PrimeMatch): Option[PreparedPrimeResult] = {
    state.find({ case (criteria, result) => primeMatch.query == criteria.query }).map(_._2)
  }
}
