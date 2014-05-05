package uk.co.scassandra.priming

import uk.co.scassandra.cqlmessages.{CqlVarchar, ColumnType, Consistency}
import com.typesafe.scalalogging.slf4j.Logging
import scala.Predef._
import uk.co.scassandra.priming.PrimeCriteria
import uk.co.scassandra.priming.PrimeQueryResult
import scala.Some
import uk.co.scassandra.priming.Prime

object PrimeQueryResultExtractor extends Logging {
  def extractPrimeCriteria(primeQueryRequest : PrimeQueryResult) : PrimeCriteria = {
    val primeConsistencies = primeQueryRequest.when.consistency match {
      case Some(list) => list.map(Consistency.fromString)
      case None => Consistency.all
    }
    PrimeCriteria(primeQueryRequest.when.query, primeConsistencies)
  }

  def extractPrimeResult(primeRequest : PrimeQueryResult) : Prime = {
    // add the deserialized JSON request to the map of prime requests
    val resultsAsList = primeRequest.then.rows.getOrElse(List())
    val then = primeRequest.then
    val result = then.result.map(Result.fromString).getOrElse(Success)
    logger.trace("Column types " + primeRequest.then.column_types)
    val columnTypes= primeRequest.then.column_types match {
      case Some(types) => types.map({
        case (columnName: String, columnTypeAsString) => (columnName, ColumnType.fromString(columnTypeAsString).getOrElse(CqlVarchar))
      })
      case _ => Map[String, ColumnType]()
    }

    // check that all the columns in the rows have a type
    val columnNamesInAllRows = resultsAsList.flatMap(row => row.keys).distinct

    val columnTypesWithMissingDefaultedToVarchar = columnNamesInAllRows.map(columnName => columnTypes.get(columnName) match {
      case Some(columnType) => (columnName, columnType)
      case None => (columnName, CqlVarchar)
    }).toMap

    logger.trace("Incoming when {}", primeRequest.when)

    val keyspace = primeRequest.when.keyspace.getOrElse("")
    val table = primeRequest.when.table.getOrElse("")


    Prime(resultsAsList, result, columnTypesWithMissingDefaultedToVarchar, keyspace, table)
  }

  def convertBackToPrimeQueryResult(allPrimes: Map[PrimeCriteria, Prime]) ={
    allPrimes.map({ case (primeCriteria, prime) => {
      val consistencies = primeCriteria.consistency.map(_.string)
      val when = When(primeCriteria.query, keyspace = Some(prime.keyspace), table = Some(prime.table), consistency = Some(consistencies))
      val rowsValuesAsString = prime.rows.map(eachRow => eachRow.map({
        case(key, valueAsAny) => (key, valueAsAny.toString)
      }))
      val columns = prime.columnTypes.map({case(colName, colType) => (colName, colType.stringRep)})
      val result = prime.result.string
      val then = Then(Some(rowsValuesAsString), result = Some(result), column_types = Some(columns))

      PrimeQueryResult(when, then)
    }})
  }
}
