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
import org.scassandra.priming.{Query, ActivityLog, PrimingJsonImplicits}
import spray.json._
import org.scassandra.AbstractIntegrationTest
import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import org.scassandra.cqlmessages.TWO

class QueryVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test clearing of query results") {
    ActivityLog.clearQueries()
    val queryString: String = "select * from people"
    session.execute(queryString)
    val svc: Req = url("http://localhost:8043/query")
    val delete = svc.DELETE
    val deleteResponse = Http(delete OK as.String)
    deleteResponse()

    val listOfQueriesResponse = Http(svc OK as.String)
    whenReady(listOfQueriesResponse) { result =>
      JsonParser(result).convertTo[List[Query]].size should equal(0)
    }
  }

  test("Test verification of a single query") {
    ActivityLog.clearQueries()
    val queryString: String = "select * from people"
    val statement = new SimpleStatement(queryString)
    statement.setConsistencyLevel(ConsistencyLevel.TWO)
    session.execute(queryString)
    val svc: Req = url("http://localhost:8043/query")
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      val queryList = JsonParser(result).convertTo[List[Query]]
      println(queryList)
      queryList.exists(_ == Query(queryString, TWO))
    }
  }
}
