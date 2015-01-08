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
package org.scassandra.server.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}

class RowsFlagParserTest extends FunSuite with Matchers {
  test("Test") {
    println(RowsFlags.GlobalTableSpec)
    println(RowsFlags.HasMorePages)
    println(RowsFlags.HasNoMetaData)

  }

  test("1 should have GlobalTableSpec and not MorePages or NoMetadata") {
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, 1) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, 1) should equal(false)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, 1) should equal(false)
  }

  test("2 should have MorePages and not GlobalTableSpec or NoMetadata") {
    val value = 2;
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, value) should equal(false)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, value) should equal(false)
  }

  test("3 should have MorePages and GlobalTableSpec but not NoMetadata") {
    val value = 3;
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, value) should equal(false)
  }

  test("7 should have MorePages and GlobalTableSpec and NoMetadata") {
    val value = 7;
    RowsFlagParser.hasFlag(RowsFlags.GlobalTableSpec, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasMorePages, value) should equal(true)
    RowsFlagParser.hasFlag(RowsFlags.HasNoMetaData, value) should equal(true)
  }
}
