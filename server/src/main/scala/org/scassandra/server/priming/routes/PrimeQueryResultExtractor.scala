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
package org.scassandra.server.priming.routes

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.cassandra.db.WriteType
import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, PrimeQuerySingle, Then, When}
import org.scassandra.server.priming._

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

object PrimeQueryResultExtractor extends LazyLogging {
  def extractPrimeCriteria(primeQueryRequest: PrimeQuerySingle): Try[PrimeCriteria] = {
    val primeConsistencies = primeQueryRequest.when.consistency.getOrElse(Consistency.all)

    primeQueryRequest.when match {
      // Prime for a specific query
      case When(Some(query), None, _, _, _) =>
        util.Success(PrimeCriteria(query, primeConsistencies, patternMatch = false))
      // Prime for a query pattern
      case When(None, Some(queryPattern), _, _, _) => util.Success(PrimeCriteria(queryPattern, primeConsistencies, patternMatch = true))
      case _ => Failure(new IllegalArgumentException("Can't specify query and queryPattern"))
    }
  }

  def extractPrimeResult(primeRequest: PrimeQuerySingle): Prime = {
    // add the deserialized JSON request to the map of prime requests
    val thenDo = primeRequest.thenDo
    val config = thenDo.config.getOrElse(Map())
    val resultsAsList = thenDo.rows.getOrElse(List())
    val result = thenDo.result.getOrElse(Success)
    val fixedDelay = primeRequest.thenDo.fixedDelay.map(FiniteDuration(_, TimeUnit.MILLISECONDS))
    val primeResult: PrimeResult = convertToPrimeResult(config, result)
    val variableTypes: List[ColumnType[_]] = then.variable_types.getOrElse(List())

    logger.trace("Column types " + primeRequest.thenDo.column_types)
    val columnTypes: Map[String, ColumnType[_]] = Defaulter.defaultColumnTypesToVarchar(primeRequest.thenDo.column_types, resultsAsList)
    logger.trace("Incoming when {}", primeRequest.when)

    val keyspace = primeRequest.when.keyspace.getOrElse("")
    val table = primeRequest.when.table.getOrElse("")

    Prime(resultsAsList, primeResult, columnTypes, keyspace, table, fixedDelay, variableTypes)
  }


  def convertToPrimeResult(config: Map[String, String], result: ResultJsonRepresentation): PrimeResult = {
    val primeResult: PrimeResult = result match {
      case Success => SuccessResult
      case ReadTimeout => ReadRequestTimeoutResult(
        config.getOrElse(ErrorConstants.ReceivedResponse, "0").toInt,
        config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
        config.getOrElse(ErrorConstants.DataPresent, "false").toBoolean
      )
      case WriteTimeout => WriteRequestTimeoutResult(
        config.getOrElse(ErrorConstants.ReceivedResponse, "0").toInt,
        config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
        WriteType.valueOf(config.getOrElse(ErrorConstants.WriteType, "SIMPLE"))
      )
      case Unavailable => UnavailableResult(
        config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
        config.getOrElse(ErrorConstants.Alive, "0").toInt)
    }
    primeResult
  }

  def convertBackToPrimeQueryResult(allPrimes: Map[PrimeCriteria, Prime]) = {
    allPrimes.map({ case (primeCriteria, prime) =>
      val when = When(Some(primeCriteria.query), keyspace = Some(prime.keyspace), table = Some(prime.table), consistency = Some(primeCriteria.consistency))

      val result = prime.result match {
        case SuccessResult => Success
        case r: ReadRequestTimeoutResult => ReadTimeout
        case r: WriteRequestTimeoutResult => WriteTimeout
      }

      val thenDo = Then(Some(prime.rows), result = Some(result), column_types = Some(prime.columnTypes))

      PrimeQuerySingle(when, thenDo)
    })
  }
}
