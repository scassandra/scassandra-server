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
package org.scassandra.priming.query

import org.scassandra.cqlmessages.{Consistency}
import org.scassandra.priming.Result
import org.scassandra.cqlmessages.types.ColumnType

/*
  These case classes are for parsing the incoming JSON for /prime-query-single
 */
case class PrimeQuerySingle(when: When, then: Then)

case class When(query: Option[String] = None, queryPattern: Option[String] = None, consistency: Option[List[Consistency]] = None, keyspace: Option[String] = None, table : Option[String] = None)

case class Then(rows: Option[List[Map[String, Any]]], result: Option[Result] = None, column_types: Option[Map[String, ColumnType[_]]] = None, fixedDelay : Option[Long] = None)

