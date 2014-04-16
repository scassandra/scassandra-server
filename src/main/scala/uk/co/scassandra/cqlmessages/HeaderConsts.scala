package org.scassandra.cqlmessages

object HeaderConsts {
  val ServerProtocolVersion : Byte = (0x82 & 0xFF).toByte
  val ClientProtocolVersion : Byte = 0x02
}
