package org.scassandra.server.cqlmessages

sealed abstract class BatchQueryKind {
  val kind: Byte
  val string: String
}

case object QueryKind extends BatchQueryKind {
  val kind: Byte = 0
  val string: String = "query"
}

case object PreparedStatementKind extends BatchQueryKind {
  val kind: Byte = 1
  val string: String = "prepared_statement"
}

object BatchQueryKind {
  def fromString(string: String): BatchQueryKind = string match {
    case QueryKind.string => QueryKind
    case PreparedStatementKind.string => PreparedStatementKind
  }

  def fromCode(code: Byte): BatchQueryKind = code match {
    case QueryKind.kind => QueryKind
    case PreparedStatementKind.kind => PreparedStatementKind
  }
}