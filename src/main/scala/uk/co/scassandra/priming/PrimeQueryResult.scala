package uk.co.scassandra.priming

case class PrimeQueryResult(when: When, then: Then)

case class Then(rows: Option[List[Map[String, String]]], result: Option[String] = None, column_types: Option[Map[String, String]] = None)

case class When(query: String, keyspace: Option[String] = None)