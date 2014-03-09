package uk.co.scassandra.priming

import com.typesafe.scalalogging.slf4j.Logging

class PrimedResults extends Logging {

  var queryToResults: Map[String, List[Map[String, String]]] = Map()

  def add(keyValue: Pair[String, List[Map[String, String]]]) = {
    queryToResults += keyValue
  }

  def get(query: String): Option[List[Map[String, String]]] = {
    logger.debug("Current primes: " + queryToResults)
    logger.debug(s"Query for |${query}|")
    queryToResults get(query)
  }

  def clear() = {
    queryToResults = Map()
  }
}

object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}