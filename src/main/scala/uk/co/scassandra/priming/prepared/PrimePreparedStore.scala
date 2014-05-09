package uk.co.scassandra.priming.prepared

import uk.co.scassandra.priming.query.{Prime, PrimeMatch}
import uk.co.scassandra.priming.routes.PrimeQueryResultExtractor

class PrimePreparedStore {

  var state: Map[String, Prime] = Map()

  def record(prime: PrimePreparedSingle) = {
    val rows = prime.then.rows.getOrElse(List())
    val colTypes = PrimeQueryResultExtractor.convertStringColumnTypes(None, rows)
    state += (prime.when.query -> Prime(prime.then.rows.get, columnTypes = colTypes))
  }

  def findPrime(primeMatch : PrimeMatch) : Option[Prime] = {
    state.get(primeMatch.query)
  }
}
