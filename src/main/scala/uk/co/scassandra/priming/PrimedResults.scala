package uk.co.scassandra.priming

import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.cqlmessages.{Consistency, ColumnType}

class PrimedResults extends Logging {

  var queryToResults: Map[PrimeCriteria, Prime] = Map()

  def getAllPrimes() : Map[PrimeCriteria, Prime] = queryToResults

  def add(primeCriteria: PrimeCriteria, rows: List[Map[String, Any]], 
          result : Result = Success, columnTypes : Map[String, ColumnType] = Map(),
          keyspace: String = "",
          table: String = "") = {
    logger.info(s"Adding prime ${primeCriteria}")
    def findExistingPrime: (PrimeCriteria) => Boolean = {
      prime => (prime.query == primeCriteria.query && (prime.consistency.intersect(primeCriteria.consistency).size > 0))
    }
    val keys = queryToResults.filterKeys(findExistingPrime).keySet.toList
    keys match {
      case head :: second :: rest => throw new IllegalStateException()
      case head :: Nil if head != primeCriteria => throw new IllegalStateException()
      case _ =>  // carry on
    }
    queryToResults += (primeCriteria -> Prime(primeCriteria.query, rows, result, columnTypes, keyspace, table))
  }

  def get(primeMatch: PrimeMatch): Option[Prime] = {
    logger.debug("Current primes: " + queryToResults)
    logger.debug(s"Query for |${primeMatch}|")
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
                  query: String,
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
  def fromString(string: String) : Result = {
    string match {
      case ReadTimeout.string => ReadTimeout
      case Unavailable.string => Unavailable
      case WriteTimeout.string => WriteTimeout
      case Success.string => Success
    }
  }
}


case class PrimeKey(query: String)

object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}