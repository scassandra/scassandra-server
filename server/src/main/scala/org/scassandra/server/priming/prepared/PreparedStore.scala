/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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
package org.scassandra.server.priming.prepared

import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages._
import org.scassandra.codec.{Execute, Prepare, Prepared}
import org.scassandra.server.priming.query.{Prime, Reply}
import org.scassandra.server.priming.{Defaulter, PrimeAddResult, PrimeAddSuccess}

import scala.collection.mutable.ListBuffer

trait PreparedStoreLookup {
  def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime]
  def apply(queryText: String, execute: Execute) : Option[Prime]
}

object PreparedStoreLookup {
  def defaultPrepared(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared): Prime = {
    val numberOfParameters = prepare.query.toCharArray.count(_ == '?')
    val variableTypes = (0 until numberOfParameters)
      .map(num => ColumnSpecWithoutTable(num.toString, DataType.Varchar).asInstanceOf[ColumnSpec]).toList

    val metadata = PreparedMetadata(
      keyspace = Some("keyspace"),
      table = Some("table"),
      columnSpec = variableTypes
    )

    Reply(preparedFactory(metadata, NoRowMetadata))
  }
}

trait PreparedStore[I <: PreparedPrimeIncoming] extends PreparedStoreLookup {
  private val primes: ListBuffer[I] = new ListBuffer[I]

  def record(prime: I): PrimeAddResult = {
    primes += prime
    PrimeAddSuccess
  }

  def retrievePrimes(): List[I] = primes.toList
  def clear() = primes.clear

  def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = {
    // Find prime by text.
    val prime = retrievePrimes().find(_.when.query.exists(_.equals(prepare.query)))
    prepared(prime, preparedFactory)
  }

  def prepared(prime: Option[I], preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = {
    prime.map { p =>
      // Prefill variable types with the rows column spec data types + varchars for any extra variables in the query.
      val numberOfParameters = p.when.query.map(_.toCharArray.count(_ == '?')).getOrElse(0)
      val dataTypes = Defaulter.defaultVariableTypesToVarChar(numberOfParameters, p.thenDo.variable_types.getOrElse(Nil))

      // build column spec from data types, we name the columns by indices.
      val variableSpec = dataTypes.indices
        .map(i => ColumnSpecWithoutTable(i.toString, dataTypes(i)).asInstanceOf[ColumnSpec])
        .toList

      val preparedMetadata = PreparedMetadata(
        keyspace = Some("keyspace"),
        table = Some("table"),
        columnSpec = variableSpec
      )
      Reply(preparedFactory(preparedMetadata, NoRowMetadata))
    }
  }
}
