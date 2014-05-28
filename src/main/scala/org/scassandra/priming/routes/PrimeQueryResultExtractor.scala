/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.priming.routes

import org.scassandra.cqlmessages.{CqlVarchar, ColumnType, Consistency}
import com.typesafe.scalalogging.slf4j.Logging
import scala.Predef._
import scala.Some
import org.scassandra.priming.query._
import scala.Some
import org.scassandra.priming.{Success, Result}
import org.scassandra.priming.query.PrimeCriteria
import org.scassandra.priming.query.PrimeQuerySingle
import org.scassandra.priming.query.When
import org.scassandra.priming.query.Then
import scala.Some
import org.scassandra.priming.query.Prime

object PrimeQueryResultExtractor extends Logging {
  def extractPrimeCriteria(primeQueryRequest : PrimeQuerySingle) : PrimeCriteria = {
    val primeConsistencies = primeQueryRequest.when.consistency match {
      case Some(list) => list
      case None => Consistency.all
    }
    PrimeCriteria(primeQueryRequest.when.query, primeConsistencies)
  }

  def extractPrimeResult(primeRequest : PrimeQuerySingle) : Prime = {
    // add the deserialized JSON request to the map of prime requests
    val resultsAsList = primeRequest.then.rows.getOrElse(List())
    val then = primeRequest.then
    val result = then.result.getOrElse(Success)
    logger.trace("Column types " + primeRequest.then.column_types)
   
    val columnTypes: Map[String, ColumnType[_]] = defaultColumnTypesToVarchar(primeRequest.then.column_types, resultsAsList)

    logger.trace("Incoming when {}", primeRequest.when)

    val keyspace = primeRequest.when.keyspace.getOrElse("")
    val table = primeRequest.when.table.getOrElse("")


    Prime(resultsAsList, result, columnTypes, keyspace, table)
  }
  
  def defaultColumnTypesToVarchar(columnTypes : Option[Map[String, ColumnType[_]]], resultsAsList: List[Map[String, Any]]) = {
    val colTypes: Map[String, ColumnType[_]] =  columnTypes.getOrElse(Map[String, ColumnType[_]]())

    // check that all the columns in the rows have a type
    val columnNamesInAllRows: List[String] = resultsAsList.flatMap(row => row.keys).distinct
    val colNamesWithOnesNotInRows = (columnNamesInAllRows ++ colTypes.keys).distinct

    val colTypesWithDefaults : Map[String, ColumnType[_]] = colNamesWithOnesNotInRows.map(columnName => colTypes.get(columnName) match {
      case Some(columnType) => (columnName, columnType)
      case None => (columnName, CqlVarchar)
    }).toMap

    colTypesWithDefaults
  }

  def convertBackToPrimeQueryResult(allPrimes: Map[PrimeCriteria, Prime]) ={
    allPrimes.map({ case (primeCriteria, prime) => {
      val when = When(primeCriteria.query, keyspace = Some(prime.keyspace), table = Some(prime.table), consistency = Some(primeCriteria.consistency))
      val then = Then(Some(prime.rows), result = Some(prime.result), column_types = Some(prime.columnTypes))

      PrimeQuerySingle(when, then)
    }})
  }
}
