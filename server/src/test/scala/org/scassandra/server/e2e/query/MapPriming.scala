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
package org.scassandra.server.e2e.query

import org.scassandra.server.AbstractIntegrationTest
import dispatch._, Defaults._
import java.util
import org.scassandra.server.cqlmessages.types.{CqlMap, CqlVarchar, CqlSet}
import org.scassandra.server.priming.query.When
import org.scassandra.server.priming.json.Success
import com.datastax.driver.core.DataType

class MapPriming extends AbstractIntegrationTest {

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test a map of strings") {
    val map = Map("one" -> "valueOne", "two" -> "valueTwo", "three" -> "valueThree")
    val whenQuery = "Test prime with cql map"
    val rows: List[Map[String, Any]] = List(Map("field" -> map))
    val mapOfVarcharToVarchar = CqlMap(CqlVarchar, CqlVarchar)
    val columnTypes  = Map("field" -> mapOfVarcharToVarchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.map(DataType.varchar(), DataType.varchar()))

    val c: Class[_] = Class.forName("java.lang.String")
    val expectedMap = new util.HashMap[String, String]() // comes back as a java set
    expectedMap.put("one","valueOne")
    expectedMap.put("two","valueTwo")
    expectedMap.put("three","valueThree")
    results.get(0).getMap("field", c, c) should equal(expectedMap)
  }
}
