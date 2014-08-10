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

import org.scassandra.cqlmessages.{Consistency}
import com.typesafe.scalalogging.slf4j.Logging
import scala.Predef._
import scala.{util, Some}
import org.scassandra.priming.query._
import org.scassandra.priming.{Defaulter, Success, Result}
import org.scassandra.priming.query.PrimeCriteria
import org.scassandra.priming.query.PrimeQuerySingle
import org.scassandra.priming.query.When
import org.scassandra.priming.query.Then
import org.scassandra.priming.query.Prime
import org.scassandra.cqlmessages.types.{CqlVarchar, ColumnType}
import scala.util.{Failure, Try}

object PrimeQueryResultExtractor extends Logging {
  def extractPrimeCriteria(primeQueryRequest : PrimeQuerySingle) : Try[PrimeCriteria] = {
    val primeConsistencies = primeQueryRequest.when.consistency match {
      case Some(list) => list
      case None => Consistency.all
    }

    primeQueryRequest.when match {
      case When(Some(query), None, _, _, _) => util.Success(PrimeCriteria(primeQueryRequest.when.query.get, primeConsistencies))
      case When(None, Some(queryPattern), _, _, _) => util.Success(PrimeCriteria(primeQueryRequest.when.queryPattern.get, primeConsistencies, true))
      case _ => Failure(new IllegalArgumentException("Can't specify query and queryPattern"))
    }
  }

  def extractPrimeResult(primeRequest : PrimeQuerySingle) : Prime = {
    // add the deserialized JSON request to the map of prime requests
    val resultsAsList = primeRequest.then.rows.getOrElse(List())
    val then = primeRequest.then
    val result = then.result.getOrElse(Success)
    logger.trace("Column types " + primeRequest.then.column_types)
   
    val columnTypes: Map[String, ColumnType[_]] = Defaulter.defaultColumnTypesToVarchar(primeRequest.then.column_types, resultsAsList)

    logger.trace("Incoming when {}", primeRequest.when)

    val keyspace = primeRequest.when.keyspace.getOrElse("")
    val table = primeRequest.when.table.getOrElse("")

    Prime(resultsAsList, result, columnTypes, keyspace, table)
  }
  


  def convertBackToPrimeQueryResult(allPrimes: Map[PrimeCriteria, Prime]) ={
    allPrimes.map({ case (primeCriteria, prime) => {
      val when = When(Some(primeCriteria.query), keyspace = Some(prime.keyspace), table = Some(prime.table), consistency = Some(primeCriteria.consistency))
      val then = Then(Some(prime.rows), result = Some(prime.result), column_types = Some(prime.columnTypes))

      PrimeQuerySingle(when, then)
    }})
  }
}
