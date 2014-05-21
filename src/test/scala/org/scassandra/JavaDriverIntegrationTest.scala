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
package org.scassandra

import org.scalatest.concurrent.ScalaFutures
import dispatch._, Defaults._
import com.datastax.driver.core.exceptions.{WriteTimeoutException, UnavailableException, ReadTimeoutException}
import org.scassandra.priming.query.When

class JavaDriverIntegrationTest extends AbstractIntegrationTest with ScalaFutures {

  test("Should by by default return empty result set for any query") {
    val result = session.execute("select * from people")

    result.all().size() should equal(0)
  }

  test("Test prime and query with many rows") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris"), Map("name"->"Alexandra"))
    prime(When(whenQuery), rows)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(1).getString("name") should equal("Alexandra")
  }

  test("Test prime and query with many columns") {
    // priming
    val whenQuery = "Test prime and query with many columns"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris", "age"->"28"), Map("name"->"Alexandra", "age"->"24"))
    prime(When(whenQuery), rows)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("28")
    results.get(1).getString("name") should equal("Alexandra")
    results.get(1).getString("age") should equal("24")
  }

  test("Test read timeout on query") {
    // priming
    val whenQuery = "read timeout query"
    prime(When(whenQuery), List(), "read_request_timeout")

    intercept[ReadTimeoutException] {
      session.execute(whenQuery)
    }
  }

  test("Test unavailable exception on query") {
    // priming
    val whenQuery = "unavailable exception query"
    prime(When(whenQuery), List(), "unavailable")

    intercept[UnavailableException] {
      session.execute(whenQuery)
    }
  }

  test("Test write timeout on query") {
    // priming
    val whenQuery = "some write query"
    prime(When(whenQuery), List(), "write_request_timeout")

    intercept[WriteTimeoutException] {
      session.execute(whenQuery)
    }
  }
}
