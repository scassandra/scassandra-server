package org.scassandra.priming

import org.scassandra.cqlmessages.{ONE, Consistency}
import com.typesafe.scalalogging.slf4j.Logging

object ActivityLog extends Logging {
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
  
  def retrievePreparedStatementExecutions(): List[PreparedStatementExecution] = preparedStatementExecutions

  def recordPrimedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[String] ) = {
    val execution: PreparedStatementExecution = PreparedStatementExecution(preparedStatementText, consistency, variables)
    logger.info("Recording " + execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def clearPreparedStatementExecutions() = {
    preparedStatementExecutions = List()
  }
}

case class Query(query: String, consistency: Consistency)
case class Connection(result: String = "success")
case class PreparedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[String] )
