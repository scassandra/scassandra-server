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
package org.scassandra.server.cqlmessages

import org.scalatest.{Matchers, FunSuite}

class ConsistencyTest extends FunSuite with Matchers {
  test("Consistency ONE") {
    ONE.code should equal(1)
    ONE.string should equal("ONE")
  }
  test("Consistency TWO") {
    TWO.code should equal(2)
    TWO.string should equal("TWO")
  }
}
