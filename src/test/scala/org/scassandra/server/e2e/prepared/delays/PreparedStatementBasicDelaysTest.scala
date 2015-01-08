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
package org.scassandra.server.e2e.prepared.delays

import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.{PrimingHelper, AbstractIntegrationTest}
import org.scassandra.server.cqlmessages._
import scala.Some
import java.nio.ByteBuffer
import akka.util.ByteString
import java.util.{UUID, Date}
import com.datastax.driver.core.utils.UUIDs
import java.net.InetAddress
import java.util
import com.datastax.driver.core.{ConsistencyLevel, Row}
import org.scassandra.server.priming._
import com.datastax.driver.core.exceptions.{UnavailableException, WriteTimeoutException, ReadTimeoutException}
import org.scalatest.BeforeAndAfter
import dispatch._, Defaults._
import spray.json._
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.priming.ConflictingPrimes
import org.scassandra.server.priming.prepared.ThenPreparedSingle
import org.scassandra.server.priming.prepared.WhenPreparedSingle
import org.scassandra.server.priming.prepared.PrimePreparedSingle

class PreparedStatementBasicDelaysTest extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  import org.scassandra.server.priming.PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Prepared statement with delay") {
    val fixedDelay: Long = 1500l
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))), fixedDelay = Some(fixedDelay))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val timeBefore = System.currentTimeMillis()
    val result = session.execute(boundStatement)
    val difference: Long = System.currentTimeMillis() - timeBefore

    //then
    val allRows = result.all()
    allRows.size() should equal(1)
    difference should be > fixedDelay
  }

  test("Prepared statement with delay and query patten") {
    val fixedDelay: Long = 1500l
    val preparedStatementText: String = ".*"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(queryPattern = Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))), fixedDelay = Some(fixedDelay))
    )
    val preparedStatement = session.prepare("select * from person where name = ?")
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val timeBefore = System.currentTimeMillis()
    val result = session.execute(boundStatement)
    val difference: Long = System.currentTimeMillis() - timeBefore

    //then
    val allRows = result.all()
    allRows.size() should equal(1)
    difference should be > fixedDelay
  }


}
