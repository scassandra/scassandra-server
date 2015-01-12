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
package org.scassandra.server.priming

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import org.scassandra.server.cqlmessages.types.{CqlBigint, CqlAscii}
import org.scassandra.server.cqlmessages.{TWO, ONE}

class ActivityLogTest extends FunSuite with Matchers with BeforeAndAfter {

  var underTest = new ActivityLog

  before {
    underTest = new ActivityLog
  }

  test("Clear connection activity log") {
    underTest.recordConnection()
    underTest.clearConnections()
    underTest.retrieveConnections().size should equal(0)
  }

  test("Clear query activity log") {
    underTest.recordQuery("select * from people", ONE)
    underTest.clearQueries()
    underTest.retrieveQueries().size should equal(0)
  }

  test("No connections should exist by default") {
    underTest.retrieveConnections().size should equal(0)
  }

  test("Store connection and retrieve connection") {
    underTest.recordConnection()
    underTest.retrieveConnections().size should equal(1)
  }

  test("Store query and retrieve connection") {
    val query: String = "select * from people"
    underTest.recordQuery(query, ONE)
    underTest.retrieveQueries().size should equal(1)
    underTest.retrieveQueries()(0).query should equal(query)
    underTest.retrieveQueries()(0).consistency should equal(ONE)
  }
  
  test("Store primed statement and retrieve primed statement") {
    underTest.clearPreparedStatementExecutions()
    val preparedStatementText = "select * from people where name = ?"
    val variables = List("Chris")
    val consistency = ONE
    val variableTypes = List(CqlAscii, CqlBigint)
    
    underTest.recordPreparedStatementExecution(preparedStatementText, consistency, variables, variableTypes)
    val preparedStatementRecord = underTest.retrievePreparedStatementExecutions()

    preparedStatementRecord.size should equal(1)
    preparedStatementRecord(0) should equal(PreparedStatementExecution(preparedStatementText, consistency, variables, variableTypes))
  }

  test("Clear prepared statement activity log") {
    underTest.recordPreparedStatementExecution("anything", TWO, List(), List())
    underTest.clearPreparedStatementExecutions()
    underTest.retrievePreparedStatementExecutions().size should equal(0)
  }
}
