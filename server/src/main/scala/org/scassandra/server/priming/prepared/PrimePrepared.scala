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
package org.scassandra.server.priming.prepared

import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming.json.{Success, ResultJsonRepresentation}

sealed trait PreparedPrimeIncoming {
  val when: WhenPrepared
}

sealed trait ThenPrepared {
  val variable_types: Option[List[ColumnType[_]]]
}

case class PrimePreparedSingle(when: WhenPrepared, thenDo: ThenPreparedSingle) extends PreparedPrimeIncoming
case class PrimePreparedMulti(when: WhenPrepared, thenDo: ThenPreparedMulti) extends PreparedPrimeIncoming

case class WhenPrepared(query: Option[String] = None,
                        queryPattern: Option[String] = None,
                        consistency: Option[List[Consistency]] = None)

case class ThenPreparedSingle(rows: Option[List[Map[String, Any]]],
                              variable_types: Option[List[ColumnType[_]]] = None,
                              column_types: Option[Map[String, ColumnType[_]]] = None,
                              result : Option[ResultJsonRepresentation] = Some(Success),
                              fixedDelay : Option[Long] = None,
                              config: Option[Map[String, String]] = None
                               ) extends ThenPrepared

case class ThenPreparedMulti(variable_types: Option[List[ColumnType[_]]] = None,
                            outcomes: List[Outcome]) extends ThenPrepared

case class Outcome(criteria: Criteria, action: Action)

case class Criteria(variable_matcher: List[VariableMatch])

case class Action(rows: Option[List[Map[String, Any]]],
                  column_types: Option[Map[String, ColumnType[_]]] = None,
                  result : Option[ResultJsonRepresentation] = Some(Success),
                  fixedDelay : Option[Long] = None,
                  config: Option[Map[String, String]] = None)

sealed trait VariableMatch {
  def test(variable: Any): Boolean
}

case class ExactMatch(matcher: Option[Any]) extends VariableMatch {
  def test(variable: Any): Boolean = {
    matcher.equals(variable)
  }
}

case object AnyMatch extends VariableMatch {
  def test(variable: Any): Boolean = {
    true
  }
}
