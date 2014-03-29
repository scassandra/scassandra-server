package uk.co.scassandra.priming

case class PrimeQueryResult(when: String, then: Then)

case class Then(rows: Option[List[Map[String, String]]], result: Option[String] = None, column_types: Option[Map[String, String]] = None)
