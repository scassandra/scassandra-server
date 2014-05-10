package uk.co.scassandra.priming.prepared

import uk.co.scassandra.priming.query.{Prime, PrimeMatch}
import uk.co.scassandra.priming.routes.PrimeQueryResultExtractor
import uk.co.scassandra.cqlmessages.{CqlVarchar, ColumnType}

class PrimePreparedStore {

  var state: Map[String, PreparedPrime] = Map()

  def record(prime: PrimePreparedSingle) = {
    val rows = prime.then.rows.getOrElse(List())
    val query = prime.when.query
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

    val colTypes = PrimeQueryResultExtractor.convertStringColumnTypes(None, rows)
    state += (query -> PreparedPrime(variableTypes, prime = Prime(rows, columnTypes = colTypes)))
  }

  def findPrime(primeMatch : PrimeMatch) : Option[PreparedPrime] = {
    state.get(primeMatch.query)
  }
}
