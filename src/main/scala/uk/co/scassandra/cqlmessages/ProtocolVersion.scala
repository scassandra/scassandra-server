package org.scassandra.cqlmessages

object ProtocolVersion {
  val ServerProtocolVersionTwo : Byte = (0x82 & 0xFF).toByte
  val ClientProtocolVersionTwo : Byte = 0x02

  val ServerProtocolVersionOne : Byte = (0x81 & 0xFF).toByte
  val ClientProtocolVersionOne : Byte = 0x01

  def protocol(clientVersion: Byte) = {
    clientVersion match {
      case VersionTwo.clientCode | VersionTwo.serverCode => VersionTwo
      case VersionOne.clientCode | VersionOne.serverCode => VersionOne
    }
  }
}

abstract class ProtocolVersion(val clientCode : Byte, val serverCode: Byte)
case object VersionTwo extends ProtocolVersion(
  ProtocolVersion.ClientProtocolVersionTwo,
  ProtocolVersion.ServerProtocolVersionTwo)

case object VersionOne extends ProtocolVersion(
  ProtocolVersion.ClientProtocolVersionOne,
  ProtocolVersion.ServerProtocolVersionOne)
