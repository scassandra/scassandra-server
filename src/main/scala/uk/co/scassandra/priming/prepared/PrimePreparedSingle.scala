package uk.co.scassandra.priming.prepared

case class PrimePreparedSingle(when: WhenPreparedSingle, then: ThenPreparedSingle)

case class WhenPreparedSingle(query: String)
case class ThenPreparedSingle(rows: Option[List[Map[String, Any]]])
