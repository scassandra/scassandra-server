package uk.co.scassandra.priming.prepared

import uk.co.scassandra.priming.query.{PrimeCriteria, Prime, PrimeMatch}
import uk.co.scassandra.priming.routes.PrimeQueryResultExtractor
import uk.co.scassandra.cqlmessages.{Consistency, CqlVarchar, ColumnType}
import uk.co.scassandra.priming.Success
import com.typesafe.scalalogging.slf4j.Logging

class PrimePreparedStore extends Logging {

  var state: Map[PrimeCriteria, PreparedPrime] = Map()

  def retrievePrimes() = state

  def record(prime: PrimePreparedSingle) = {
    val rows = prime.then.rows.getOrElse(List())
    val query = prime.when.query
    val result = prime.then.result.getOrElse(Success)
    val numberOfParameters = query.toCharArray.filter(_ == '?').size
    val variableTypes = prime.then.variable_types match {
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
    val primeToStore: PreparedPrime = PreparedPrime(variableTypes, prime = Prime(rows, columnTypes = colTypes, result = result))

    val consistencies = prime.when.consistency.getOrElse(Consistency.all)
    val primeCriteria = PrimeCriteria(query, consistencies)
    logger.info(s"Storing Prime for Prepared Statement $primeToStore")
    state += (primeCriteria -> primeToStore)
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
