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
import org.scassandra.server.cqlmessages.types.{CqlText, CqlBigint, CqlAscii}
import org.scassandra.server.cqlmessages.{LOGGED, Consistency, TWO, ONE}

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
    underTest.retrieveQueries().head.query should equal(query)
    underTest.retrieveQueries().head.consistency should equal(ONE)
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
    preparedStatementRecord.head should equal(PreparedStatementExecution(preparedStatementText, consistency, variables, variableTypes))
  }

  test("Clear prepared statement activity log") {
    underTest.recordPreparedStatementExecution("anything", TWO, List(), List())
    underTest.clearPreparedStatementExecutions()
    underTest.retrievePreparedStatementExecutions().size should equal(0)
  }
  
  test("Records query parameters") {
    underTest.recordQuery("query", ONE, List("Hello"), List(CqlText))

    underTest.retrieveQueries() should equal(List(Query("query", ONE, List("Hello"), List(CqlText))))
  }

  test("Record batch execution") {
    val consistency: Consistency = ONE
    val statements: List[BatchStatement] = List(BatchStatement("select * from hello"))
    val execution: BatchExecution = BatchExecution(statements, consistency, LOGGED)
    underTest.recordBatchExecution(execution)

    val executions: List[BatchExecution] = underTest.retrieveBatchExecutions()

    executions should equal(List(execution))
  }

  test("Clear batch execution") {
    underTest.recordBatchExecution(BatchExecution(List(BatchStatement("select * from hello")), ONE, LOGGED))

    underTest.clearBatchExecutions()

    underTest.retrieveBatchExecutions() should equal(List())
  }
}
