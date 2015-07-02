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
package org.scassandra.server.priming.query

import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.priming.ResultJsonRepresentation
import org.scassandra.server.cqlmessages.types.ColumnType

/*
  These case classes are for parsing the incoming JSON for /prime-query-single

  Really variable_types make more sense in the When but for reason I put them in the
  Then for prepared statements and I didn't want the two to be inconsistent
 */
case class PrimeQuerySingle(when: When, thenDo: Then)

case class When(query: Option[String] = None,
                queryPattern: Option[String] = None,
                consistency: Option[List[Consistency]] = None,
                keyspace: Option[String] = None,
                table: Option[String] = None
                 )

case class Then(rows: Option[List[Map[String, Any]]],
                result: Option[ResultJsonRepresentation] = None,
                column_types: Option[Map[String, ColumnType[_]]] = None,
                fixedDelay: Option[Long] = None,
                config: Option[Map[String, String]] = None,
                variable_types: Option[List[ColumnType[_]]] = None)

