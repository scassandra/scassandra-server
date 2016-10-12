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
package org.scassandra.server.e2e.query

import com.datastax.driver.core.{CodecRegistry, DataType, ProtocolVersion, TupleType}
import org.scassandra.codec.datatype.{DataType => DType}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

class TuplePriming extends AbstractIntegrationTest {

  def tt(dataType: DataType*): TupleType =
    TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, dataType:_*)

  test("Test a tuple<varchar, ascii>") {
    val tuple = ("one", "two")
    val whenQuery = "Test prime with tuple<varchar, ascii>"
    val rows: List[Map[String, Any]] = List(Map("field" -> tuple))
    val columnTypes = Map("field" -> DType.Tuple(DType.Varchar, DType.Ascii))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()

    val tupleType = tt(DataType.varchar(), DataType.ascii())
    val expectedTuple = tupleType.newValue(tuple._1, tuple._2)

    singleRow.getColumnDefinitions.getType("field") should equal(tupleType)
    singleRow.getTupleValue("field") should equal(expectedTuple)
  }
}
