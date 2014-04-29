package uk.co.scassandra.priming

import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.cqlmessages.{Consistency, ColumnType}

class PrimedResults extends Logging {

  var queryToResults: Map[PrimeCriteria, Prime] = Map()

  def getAllPrimes() : Map[PrimeCriteria, Prime] = queryToResults

  def add(criteria: PrimeCriteria, prime: Prime) = {
    logger.debug(s"Adding prime with criteria $criteria")

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

case class PrimeMatch(query: String, consistency: Consistency)

case class Prime(
                  rows: List[Map[String, Any]],
                  result: Result = Success,
                  columnTypes: Map[String, ColumnType] = Map(),
                  keyspace: String = "",
                  table: String = ""
                  )

abstract class Result(val string: String)

case object Success extends Result("success")

case object ReadTimeout extends Result("read_request_timeout")

case object Unavailable extends Result("unavailable")

case object WriteTimeout extends Result("write_request_timeout")

object Result {
  def fromString(string: String): Result = {
    string match {
      case ReadTimeout.string => ReadTimeout
      case Unavailable.string => Unavailable
      case WriteTimeout.string => WriteTimeout
      case Success.string => Success
    }
  }
}


object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}