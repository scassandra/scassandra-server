package org.scassandra.server.cqlmessages

sealed abstract class BatchType {
  val code: Byte
  val string: String
}

object LOGGED extends BatchType {
  val code: Byte = 0
  val string: String = "LOGGED"
}
object UNLOGGED extends BatchType {
  val code: Byte = 1
  val string: String = "UNLOGGED"
}
object COUNTER extends BatchType {
  val code: Byte = 2
  val string: String = "COUNTER"
}

object BatchType {
  def fromString(batchType: String): BatchType = batchType match {
    case LOGGED.string => LOGGED
    case UNLOGGED.string => UNLOGGED
    case COUNTER.string => COUNTER
  }

  def fromCode(code: Byte): BatchType = {
    code match {
      case LOGGED.code => LOGGED
      case UNLOGGED.code => UNLOGGED
      case COUNTER.code => COUNTER
    }
  }
}


