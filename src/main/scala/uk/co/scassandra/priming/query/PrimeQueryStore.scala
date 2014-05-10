package uk.co.scassandra.priming.query

import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.cqlmessages.{ONE, Consistency, ColumnType}
import uk.co.scassandra.priming.{Success, Result}

class PrimeQueryStore extends Logging {

  var queryToResults: Map[PrimeCriteria, Prime] = Map()

  def getAllPrimes() : Map[PrimeCriteria, Prime] = queryToResults

  def add(criteria: PrimeCriteria, prime: Prime) = {
    logger.info(s"Adding prime with criteria $criteria and prime result ${prime}")

    def intersectsExistingCriteria: (PrimeCriteria) => Boolean = {
      existing => existing.query == criteria.query && existing.consistency.intersect(criteria.consistency).size > 0
    }

    val intersectingCriteria = queryToResults.filterKeys(intersectsExistingCriteria).keySet.toList
    intersectingCriteria match {
      // exactly one intersecting criteria: if the criteria is the newly passed one, this is just an override. Otherwise, conflict.
      case head :: Nil if head != criteria => throw new IllegalStateException()
      // two or more intersecting criteria: this means one or more conflicts
      case head :: second :: rest => throw new IllegalStateException()
      // all other cases: carry on
      case _ =>
    }
    queryToResults += (criteria -> prime)
  }

  def get(primeMatch: PrimeMatch): Option[Prime] = {
    logger.debug("Current primes: " + queryToResults)
    logger.debug(s"Query for |$primeMatch|")
    def findPrime: ((PrimeCriteria, Prime)) => Boolean = {
      entry => entry._1.query == primeMatch.query &&
        entry._1.consistency.contains(primeMatch.consistency)
    }
    queryToResults.find(findPrime).map(_._2)
  }

  def getPrimeCriteriaByQuery(query: String): List[PrimeCriteria] = {
    queryToResults.keys.filter(primeCriteria => primeCriteria.query == query).toList
  }

  def clear() = {
    queryToResults = Map()
  }
}


case class PrimeCriteria(query: String, consistency: List[Consistency])

case class PrimeMatch(query: String, consistency: Consistency = ONE)

case class Prime(
                  rows: List[Map[String, Any]] = List(),
                  result: Result = Success,
                  columnTypes: Map[String, ColumnType] = Map(),
                  keyspace: String = "",
                  table: String = ""
                  )

object PrimeQueryStore {
  def apply() = {
    new PrimeQueryStore()
  }
}