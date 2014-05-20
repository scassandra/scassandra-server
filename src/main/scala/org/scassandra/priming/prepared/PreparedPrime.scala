package org.scassandra.priming.prepared

import org.scassandra.cqlmessages.ColumnType
import org.scassandra.priming.query.Prime

case class PreparedPrime(
                  variableTypes: List[ColumnType[_]] = List(),
                  prime: Prime = Prime()
                  )