package uk.co.scassandra.priming.prepared

import uk.co.scassandra.cqlmessages.ColumnType
import uk.co.scassandra.priming.{Success, Result}

case class PrimePreparedSingle(when: WhenPreparedSingle, then: ThenPreparedSingle)

case class WhenPreparedSingle(query: String)
case class ThenPreparedSingle(rows: Option[List[Map[String, Any]]],
                              variable_types: Option[List[ColumnType[_]]] = None,
                              column_types: Option[Map[String, ColumnType[_]]] = None,
                              result : Option[Result] = Some(Success)
                               )
