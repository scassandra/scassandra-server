package uk.co.scassandra.priming.prepared

import uk.co.scassandra.priming.query.{Prime, PrimeMatch}

class PrimePreparedStore {

  var state: Map[String, Prime] = Map()

  def record(prime: PrimePreparedSingle) = {
    state += (prime.when.query -> Prime(prime.then.rows.get))
  }

  def findPrime(primeMatch : PrimeMatch) : Option[Prime] = {
    state.get(primeMatch.query)
  }
}
