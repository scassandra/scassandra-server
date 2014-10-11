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
package org.scassandra.e2e

import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import org.scassandra.priming.{PreparedStatementExecution, Query, ActivityLog, PrimingJsonImplicits}
import spray.json._
import org.scassandra.AbstractIntegrationTest
import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import org.scassandra.cqlmessages.ONE

class PreparedStatementExecutionVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  val primePreparedSinglePath = "http://localhost:8043/prepared-statement-execution"

  before {
    val svc = url(primePreparedSinglePath).DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test clearing of prepared statement executions") {
    //todo: clean activity log
    val queryString = "select * from people where name = ?"
    val preparedStatement = session.prepare(queryString);
    val boundStatement = preparedStatement.bind("Chris")
    session.execute(boundStatement)
    val svc: Req = url(primePreparedSinglePath)
    val delete = svc.DELETE
    val deleteResponse = Http(delete OK as.String)
    deleteResponse()

    val listOfPreparedStatementExecutions = Http(svc OK as.String)
    whenReady(listOfPreparedStatementExecutions) { result =>
      JsonParser(result).convertTo[List[PreparedStatementExecution]].size should equal(0)
    }
  }

  test("Test verification of a single prepared statement execution") {
    //todo: clean activity log
    val queryString: String = "select * from people where name = ?"
    val preparedStatement = session.prepare(queryString);
    val boundStatement = preparedStatement.bind("Chris")
    session.execute(boundStatement)

    val svc: Req = url(primePreparedSinglePath)
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      println(result)
      val preparedStatementExecutions = JsonParser(result).convertTo[List[PreparedStatementExecution]]
      preparedStatementExecutions.size should equal(1)
      preparedStatementExecutions(0) should equal(PreparedStatementExecution(queryString, ONE, List()))
    }
  }
}
