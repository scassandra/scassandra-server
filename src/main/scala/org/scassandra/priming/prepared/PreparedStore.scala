package org.scassandra.priming.prepared

import org.scassandra.priming.PrimeAddResult
import org.scassandra.priming.query.{PrimeCriteria, PrimeMatch}

trait PreparedStore {
  var state: Map[PrimeCriteria, PreparedPrime] = Map()
  def record(prime: PrimePreparedSingle) : PrimeAddResult
  def retrievePrimes(): Map[PrimeCriteria, PreparedPrime] = state
  def clear() = {
    state = Map()
  }
}

trait PreparedStoreLookup {
  def findPrime(primeMatch : PrimeMatch) : Option[PreparedPrime]
}
