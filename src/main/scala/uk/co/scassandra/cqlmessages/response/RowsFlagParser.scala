package org.scassandra.cqlmessages.response

object RowsFlagParser {
  def hasFlag(flag : Int, value : Int) = {
    (value & flag) == flag
  }
}

object RowsFlags {
  val GlobalTableSpec = 1 << 0
  val HasMorePages = 1 << 1
  val HasNoMetaData = 1 << 2
}
