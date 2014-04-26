package uk.co.scassandra.priming

import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.cqlmessages.{Consistency, ColumnType}

class PrimedResults extends Logging {

  var queryToResults: Map[PrimeCriteria, Prime] = Map()

  def add(primeCriteria: PrimeCriteria, rows: List[Map[String, Any]], 
          result : Result = Success, columnTypes : Map[String, ColumnType] = Map(),
          keyspace: String = "",
          table: String = "") = {
    logger.info(s"Adding prime $primeCriteria")
    def findExistingPrime: (PrimeCriteria) => Boolean = {
      prime => (prime.query == primeCriteria.query && (prime.consistency.intersect(primeCriteria.consistency).size > 0))
    }
    val keys = queryToResults.filterKeys(findExistingPrime).keySet.toList
    keys match {
      case head :: second :: rest => throw new IllegalStateException()
      case head :: Nil if head != primeCriteria => throw new IllegalStateException()
      case _ =>  // carry on
    }
    queryToResults += (primeCriteria -> Prime(rows, result, columnTypes, keyspace, table))
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

abstract class Result

case object Success extends Result

case object ReadTimeout extends Result

case object Unavailable extends Result

case object WriteTimeout extends Result

object Result {
  def fromString(string: String) : Result = {
    string match {
      case "read_request_timeout" => ReadTimeout
      case "unavailable" => Unavailable
      case "write_request_timeout" => WriteTimeout
      case "success" => Success
    }
  }
}


case class PrimeKey(query: String)

object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}