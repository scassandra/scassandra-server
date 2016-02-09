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
package org.scassandra.server.e2e.prepared

import java.util

import com.datastax.driver.core.Row
import dispatch._, Defaults._
import org.scassandra.server.cqlmessages.types._
import org.scassandra.server.priming.json.PrimingJsonImplicits
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPrepared}
import spray.json._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.{PrimingHelper, AbstractIntegrationTest}

class PreparedStatementWithCollectionsTest  extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Text map as a variable and column type") {
    // given
    val preparedStatementText = "Select * from people where id = ? and map = ?"
    val primedRow = Map[String, Any]("map_column" -> Map("the" -> "result"))
    val mapVariale = new util.HashMap[String, String]()
    mapVariale.put("one", "ONE")
    mapVariale.put("two", "TWO")
    val variableTypes: List[ColumnType[_]] = List(CqlInt, new CqlMap(CqlText, CqlText))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(primedRow)),
        variable_types = Some(variableTypes),
        column_types = Some(Map[String, ColumnType[_]]("map_column" -> new CqlMap(CqlText, CqlText)))
      )
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(Int.box(1), mapVariale)
    val result = session.execute(boundStatement)

    // then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)

    val resultMap = resultRow.getMap("map_column", classOf[String], classOf[String])
    resultMap.get("the") should equal("result")
  }
}
