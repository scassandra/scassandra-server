package uk.co.scassandra.priming.prepared

import uk.co.scassandra.priming.query._
import uk.co.scassandra.priming.routes.PrimeQueryResultExtractor
import uk.co.scassandra.cqlmessages.{Consistency, CqlVarchar}
import uk.co.scassandra.priming.Success
import com.typesafe.scalalogging.slf4j.Logging
import uk.co.scassandra.priming.query.PrimeCriteria
import uk.co.scassandra.priming.query.PrimeMatch
import scala.Some
import uk.co.scassandra.priming.query.Prime

class PrimePreparedStore extends Logging {

  val validator: PrimeValidator = PrimeValidator()

  var state: Map[PrimeCriteria, PreparedPrime] = Map()

  def retrievePrimes() = state

  def record(prime: PrimePreparedSingle) : PrimeAddResult= {
    val rows = prime.then.rows.getOrElse(List())
    val query = prime.when.query
    val result = prime.then.result.getOrElse(Success)
    val numberOfParameters = query.toCharArray.filter(_ == '?').size
    val variableTypesDefaultedToVarchar = prime.then.variable_types match {
      case Some(varTypes) => {
        val defaults = (0 until numberOfParameters).map(num => CqlVarchar).toList
        varTypes ++ (defaults drop varTypes.size)
      }
      case None => {
        (0 until numberOfParameters).map(num => CqlVarchar).toList
      }
    }
    val providedColTypes = prime.then.column_types
    val colTypes = PrimeQueryResultExtractor.convertStringColumnTypes(providedColTypes, rows)
    val primeToStore: PreparedPrime = PreparedPrime(variableTypesDefaultedToVarchar, prime = Prime(rows, columnTypes = colTypes, result = result))

    val consistencies = prime.when.consistency.getOrElse(Consistency.all)
    val primeCriteria = PrimeCriteria(query, consistencies)



    validator.validate(primeCriteria, primeToStore.prime, state.map( existingPrime => (existingPrime._1, existingPrime._2.prime)  ) ) match {
      case PrimeAddSuccess => {
        logger.info(s"Storing prime for prepared statement $primeToStore")
        state += (primeCriteria -> primeToStore)
        PrimeAddSuccess
      }
      case notSuccess: PrimeAddResult => {
        logger.info(s"Storing prime for prepared statement $primeToStore failed due to $notSuccess")
        notSuccess
      }
    }
  }

  def findPrime(primeMatch : PrimeMatch) : Option[PreparedPrime] = {
    def findPrime: ((PrimeCriteria, PreparedPrime)) => Boolean = {
      entry => entry._1.query == primeMatch.query &&
        entry._1.consistency.contains(primeMatch.consistency)
    }
    state.find(findPrime).map(_._2)
  }

  def clear() = {
    state = Map()
  }
}
