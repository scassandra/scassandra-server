package uk.co.scassandra.priming

case class PrimeQuerySingle(when: When, then: Then)

case class Then(rows: Option[List[Map[String, Any]]], result: Option[String] = None, column_types: Option[Map[String, String]] = None)

case class When(query: String, consistency: Option[List[String]] = None, keyspace: Option[String] = None, table : Option[String] = None)
