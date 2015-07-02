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
package org.scassandra.server.e2e.querybuilder

import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.cqlmessages.ONE
import org.scassandra.server.cqlmessages.types.{CqlText, CqlInt, CqlVarchar}
import org.scassandra.server.priming.Query
import org.scassandra.server.priming.query.{Then, When}
import org.scassandra.server.PrimingHelper._

class QueryWithParametersTest extends AbstractIntegrationTest {

  before {
    clearQueryPrimes()
    clearRecordedQueries()
  }

  test("Prime using text parameter") {
    val query = "select * from people where name = ?"
    val rowOne = Map("name" -> "Chris", "age" -> 15)
    val columnTypes = Map(
      "name" -> CqlVarchar,
      "age" -> CqlInt)
    val variableTypes = List(CqlText)
    val then = Then(rows = Some(List(rowOne)), column_types = Some(columnTypes), variable_types = Some(variableTypes))
    prime(When(query = Some(query)), then)

    val result = session.execute(query, "chris")

    getRecordedQueries() shouldEqual List(Query(query, ONE, List("chris"), List(CqlText)))
  }

  test("Prime using int parameter") {
    val query = "select * from people where age = ?"
    val rowOne = Map("name" -> "Chris", "age" -> 15)
    val columnTypes = Map(
      "name" -> CqlVarchar,
      "age" -> CqlInt)
    val variableTypes = List(CqlInt)
    val then = Then(Some(List(rowOne)), column_types = Some(columnTypes), variable_types = Some(variableTypes))
    prime(When(query = Some(query)), then)

    val result = session.execute(query, new Integer(15))

    getRecordedQueries() shouldEqual List(Query(query, ONE, List(15), List(CqlInt)))
  }
}
