package uk.co.scassandra.priming

import uk.co.scassandra.cqlmessages.{ONE, Consistency}

object ActivityLog {
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
    preparedStatementExecutions = preparedStatementExecutions ::: PreparedStatementExecution(preparedStatementText, consistency, variables) :: Nil
  }

  def clearPreparedStatementExecutions() = {
    preparedStatementExecutions = List()
  }
}

case class Query(query: String, consistency: Consistency)
case class Connection(result: String = "success")
case class PreparedStatementExecution(preparedStatementText: String, consistency: Consistency, variables: List[String] )
