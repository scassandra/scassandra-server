package uk.co.scassandra.priming

object ActivityLog {

  var connections : List[Connection] = List()

  def recordQuery() = ???

  def recordConnection() = {
    connections = connections ::: Connection() :: Nil
  }

  def retrieveConnections() : List[Connection] = connections

  def retrieveQueries() = ???

  def clearConnections() = {
    connections = List()
  }
}

case class Query(query: String)
case class Connection(result: String = "success")
