package uk.co.scassandra.priming.routes

import uk.co.scassandra.cqlmessages.{CqlVarchar, ColumnType, Consistency}
import com.typesafe.scalalogging.slf4j.Logging
import scala.Predef._
import scala.Some
import uk.co.scassandra.priming.query._
import scala.Some
import uk.co.scassandra.priming.{Success, Result}
import uk.co.scassandra.priming.query.PrimeCriteria
import uk.co.scassandra.priming.query.PrimeQuerySingle
import uk.co.scassandra.priming.query.When
import uk.co.scassandra.priming.query.Then
import scala.Some
import uk.co.scassandra.priming.query.Prime

object PrimeQueryResultExtractor extends Logging {
  def extractPrimeCriteria(primeQueryRequest : PrimeQuerySingle) : PrimeCriteria = {
    val primeConsistencies = primeQueryRequest.when.consistency match {
      case Some(list) => list.map(Consistency.fromString)
      case None => Consistency.all
    }
    PrimeCriteria(primeQueryRequest.when.query, primeConsistencies)
  }

  def extractPrimeResult(primeRequest : PrimeQuerySingle) : Prime = {
    // add the deserialized JSON request to the map of prime requests
    val resultsAsList = primeRequest.then.rows.getOrElse(List())
    val then = primeRequest.then
    val result = then.result.map(Result.fromString).getOrElse(Success)
    logger.trace("Column types " + primeRequest.then.column_types)
   
    val columnTypes: Map[String, ColumnType[_]] = convertStringColumnTypes(primeRequest.then.column_types, resultsAsList)

    logger.trace("Incoming when {}", primeRequest.when)

    val keyspace = primeRequest.when.keyspace.getOrElse("")
    val table = primeRequest.when.table.getOrElse("")


    Prime(resultsAsList, result, columnTypes, keyspace, table)
  }
  
  def convertStringColumnTypes(columnTypes : Option[Map[String, ColumnType[_]]], resultsAsList: List[Map[String, Any]]) = {
    val colTypes =  columnTypes.getOrElse(Map[String, ColumnType[_]]())

    // check that all the columns in the rows have a type
    val columnNamesInAllRows = resultsAsList.flatMap(row => row.keys).distinct

    val colTypesWithDefaults : Map[String, ColumnType[_]] = columnNamesInAllRows.map(columnName => colTypes.get(columnName) match {
      case Some(columnType) => (columnName, columnType)
      case None => (columnName, CqlVarchar)
    }).toMap

    colTypesWithDefaults
  }

  def convertBackToPrimeQueryResult(allPrimes: Map[PrimeCriteria, Prime]) ={
    allPrimes.map({ case (primeCriteria, prime) => {
      val consistencies = primeCriteria.consistency.map(_.string)
      val when = When(primeCriteria.query, keyspace = Some(prime.keyspace), table = Some(prime.table), consistency = Some(consistencies))
      val rowsValuesAsString = prime.rows.map(eachRow => eachRow.map({
        case(key, valueAsAny) => (key, valueAsAny.toString)
      }))
      val result = prime.result.string
      val then = Then(Some(rowsValuesAsString), result = Some(result), column_types = Some(prime.columnTypes))

      PrimeQuerySingle(when, then)
    }})
  }
}
