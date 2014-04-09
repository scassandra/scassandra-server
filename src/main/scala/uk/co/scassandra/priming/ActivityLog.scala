package uk.co.scassandra.priming

import org.scassandra.cqlmessages.Consistency

object ActivityLog {

  var connections : List[Connection] = List()
  var queries : List[Query] = List()

  def recordQuery(query: String, consistency: Consistency) = {
    queries = queries ::: Query(query, consistency.string) :: Nil
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
}

case class Query(query: String, consistency: String)
case class Connection(result: String = "success")
