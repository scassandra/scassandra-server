package org.scassandra.cqlmessages

object ProtocolVersions {
  val ServerProtocolVersionTwo : Byte = (0x82 & 0xFF).toByte
  val ClientProtocolVersionTwo : Byte = 0x02

  val ServerProtocolVersionOne : Byte = (0x81 & 0xFF).toByte
  val ClientProtocolVersionOne : Byte = 0x01

  def serverVersionForClientVersion(clientVersion: Byte) = {
    clientVersion match {
      case ClientProtocolVersionTwo => ServerProtocolVersionTwo
      case ClientProtocolVersionOne => ServerProtocolVersionOne
    }
  }
}
