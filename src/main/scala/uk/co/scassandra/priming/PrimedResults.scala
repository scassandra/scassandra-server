package uk.co.scassandra.priming

import com.typesafe.scalalogging.slf4j.Logging

class PrimedResults extends Logging {

  var queryToResults: Map[String, Prime] = Map()

  def add(keyValue: String, rows: List[Map[String, String]], result : Result = Success) = {
    logger.info(s"Adding prime ${keyValue}")
    queryToResults += (keyValue -> Prime(keyValue, rows, result))
  }

  def get(query: String): Option[Prime] = {
    logger.debug("Current primes: " + queryToResults)
    logger.debug(s"Query for |${query}|")
    queryToResults get(query)
  }

  def clear() = {
    queryToResults = Map()
  }
}

case class Prime(query: String, rows: List[Map[String, String]], result : Result = Success)

abstract class Result
case object Success extends Result
case object ReadTimeout extends Result
case object Unavailable extends Result

object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}