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

import org.scassandra.cqlmessages.ColumnType

case class PrimeQuerySingle(when: When, then: Then)

case class Then(rows: Option[List[Map[String, Any]]], result: Option[String] = None, column_types: Option[Map[String, ColumnType[_]]] = None)

case class When(query: String, consistency: Option[List[String]] = None, keyspace: Option[String] = None, table : Option[String] = None)
