package priming

object PrimedResults {
  def apply() = {
    new PrimedResults()
  }
}

class PrimedResults {

  var queryToResults: Map[String, List[Map[String, String]]] = Map()

  def add(keyValue: Pair[String, List[Map[String, String]]]) = {
    queryToResults += keyValue
  }

  def get(query: String): List[Map[String, String]] = {
    queryToResults getOrElse(query, Nil)
  }

  def clear() = {
    queryToResults = Map()
  }
}
