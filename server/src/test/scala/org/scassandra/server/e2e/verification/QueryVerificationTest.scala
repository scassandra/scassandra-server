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
package org.scassandra.server.e2e.verification

import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.PrimingHelper._
import org.scassandra.server.cqlmessages.TWO
import org.scassandra.server.priming.Query

class QueryVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  before {
    clearQueryPrimes()
  }

  test("Test clearing of query results") {
    session.execute("select * from people")

    clearRecordedQueries()

    getRecordedQueries() shouldEqual List()
  }

  test("Test verification of a single query") {
    val queryString: String = "select * from people"
    val statement = new SimpleStatement(queryString)
    statement.setConsistencyLevel(ConsistencyLevel.TWO)

    session.execute(statement)

    val queryList = getRecordedQueries()
    queryList shouldEqual List(Query(queryString, TWO))
  }
}
