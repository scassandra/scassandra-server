package org.scassandra.priming.prepared

import org.scassandra.priming.query.PrimeMatch

class CompositePreparedPrimeStore(firstPrimeStore: PreparedStoreLookup, secondPrimeStore : PreparedStoreLookup) extends PreparedStoreLookup {
  override def findPrime(primeMatch: PrimeMatch): Option[PreparedPrime] = {
    firstPrimeStore.findPrime(primeMatch).orElse(secondPrimeStore.findPrime(primeMatch))
  }
}
