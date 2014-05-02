package uk.co.scassandra.cqlmessages.response

object ResultKinds {
  val VoidResult : Int = 0x0001
  val Rows: Int = 0x0002
  val SetKeyspace : Int = 0x0003
  val Prepared: Int = 0x0004
}
