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
package org.scassandra.server.priming

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.cqlmessages.Consistency
import org.scassandra.server.cqlmessages.types.ColumnType

class ActivityLog extends LazyLogging {
  var connections : List[Connection] = List()
  var queries : List[Query] = List()
  var preparedStatementExecutions : List[PreparedStatementExecution] = List()

  def recordQuery(query: String, consistency: Consistency) = {
    queries = queries ::: Query(query, consistency) :: Nil
  }

  def recordConnection() = {
    connections = connections ::: Connection() :: Nil
  }

  def retrieveConnections() : List[Connection] = connections

  def retrieveQueries() : List[Query] = queries

  def clearConnections() = {
    connections = List()
  }

  def clearQueries() = {
    queries = List()
  }
  
  def retrievePreparedStatementExecutions(): List[PreparedStatementExecution] = {
    preparedStatementExecutions
  }

  def recordPreparedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[Any], variableTypes: List[ColumnType[_]]): Unit = {
    val execution: PreparedStatementExecution = PreparedStatementExecution(preparedStatementText, consistency, variables, variableTypes)
    logger.info("Recording " + execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def recordPreparedStatementExecution(execution: PreparedStatementExecution): Unit = {
    logger.info("Recording " + execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def clearPreparedStatementExecutions() = {
    preparedStatementExecutions = List()
  }
}

case class Query(query: String, consistency: Consistency)
case class Connection(result: String = "success")
case class PreparedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[Any], variableTypes: List[ColumnType[_]])
