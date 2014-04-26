package uk.co.scassandra.cqlmessages.response

object ResultKinds {
  val SetKeyspace : Int = 0x0003
  val VoidResult : Int = 0x0001
  val Rows: Int = 0x0002
}
