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
import org.scassandra.server.cqlmessages.{BatchType, Consistency}
import org.scassandra.server.cqlmessages.types.ColumnType

class ActivityLog extends LazyLogging {

  var connections : List[Connection] = List()
  var queries : List[Query] = List()
  var preparedStatementPreparations : List[PreparedStatementPreparation] = List()
  var preparedStatementExecutions : List[PreparedStatementExecution] = List()
  var batchExecutions: List[BatchExecution] = List()

  /*  Query activity logging */

  def clearQueries() = {
    queries = List()
  }

  def retrieveQueries() : List[Query] = queries
  
  def recordQuery(query: String, consistency: Consistency, variables: List[Any] = List(), variableTypes: List[ColumnType[_]] = List()) = {
    queries = queries ::: Query(query, consistency, variables, variableTypes) :: Nil
  }

  /*  Connection activity logging */

  def recordConnection() = {
    connections = connections ::: Connection() :: Nil
  }

  def retrieveConnections(): List[Connection] = connections

  def clearConnections(): Unit = {
    connections = List()
  }

  /*  PreparedStatementPreparation activity logging */

  def recordPreparedStatementPreparation(activity: PreparedStatementPreparation) = {
    logger.info("Recording {}", activity)
    preparedStatementPreparations = preparedStatementPreparations ::: activity :: Nil
  }

  def retrievePreparedStatementPreparations(): List[PreparedStatementPreparation] = {
    preparedStatementPreparations
  }

  def clearPreparedStatementPreparations() = {
    logger.info("Clearing prepared statement preparations")
    preparedStatementPreparations = List()
  }

  /*  PreparedStatementExecution activity logging */

  def recordPreparedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[Any], variableTypes: List[ColumnType[_]]): Unit = {
    val execution: PreparedStatementExecution = PreparedStatementExecution(preparedStatementText, consistency, variables, variableTypes)
    logger.info("Recording {}",execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def recordPreparedStatementExecution(execution: PreparedStatementExecution): Unit = {
    logger.info("Recording {}", execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def retrievePreparedStatementExecutions(): List[PreparedStatementExecution] = {
    preparedStatementExecutions
  }

  def clearPreparedStatementExecutions() = {
    preparedStatementExecutions = List()
  }

  /*  BatchExecution activity logging */

  def recordBatchExecution(batchExecution: BatchExecution): Unit = {
    logger.info("Recording {}", batchExecutions)
    batchExecutions = batchExecutions ::: batchExecution :: Nil
  }

  def retrieveBatchExecutions(): List[BatchExecution] = {
    batchExecutions
  }

  def clearBatchExecutions(): Unit = {
    batchExecutions = List()
  }
}

case class Query(query: String, consistency: Consistency, variables: List[Any] = List(), variableTypes: List[ColumnType[_]] = List())
case class Connection(result: String = "success")
case class PreparedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[Any], variableTypes: List[ColumnType[_]])
case class BatchQuery(query: String)
case class BatchExecution(batchQueries: List[BatchQuery], consistency: Consistency, batchType: BatchType)
case class PreparedStatementPreparation(preparedStatementText: String)
