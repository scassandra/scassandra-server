package uk.co.scassandra.priming

import com.typesafe.scalalogging.slf4j.Logging
import org.scassandra.cqlmessages.ColumnType

class PrimedResults extends Logging {

  var queryToResults: Map[When, Prime] = Map()

  def add(keyValue: When, rows: List[Map[String, Any]], result : Result = Success, columnTypes : Map[String, ColumnType] = Map()) = {
    logger.info(s"Adding prime ${keyValue}")
    queryToResults += (keyValue -> Prime(keyValue.query, rows, result, columnTypes))
  }

  def get(query: When): Option[Prime] = {
    logger.debug("Current primes: " + queryToResults)
    logger.debug(s"Query for |${query}|")
    queryToResults get(query)
  }

  def clear() = {
    queryToResults = Map()
  }
}

case class Prime(query: String, rows: List[Map[String, Any]], result : Result = Success, columnTypes : Map[String, ColumnType] = Map())

abstract class Result
case object Success extends Result
case object ReadTimeout extends Result
case object Unavailable extends Result
case object WriteTimeout extends Result


object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}