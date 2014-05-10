package uk.co.scassandra.priming.prepared

import uk.co.scassandra.cqlmessages.ColumnType

case class PrimePreparedSingle(when: WhenPreparedSingle, then: ThenPreparedSingle)

case class WhenPreparedSingle(query: String)
case class ThenPreparedSingle(rows: Option[List[Map[String, Any]]],
                              variable_types: Option[List[ColumnType]] = None
                               )
