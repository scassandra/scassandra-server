package org.scassandra.cqlmessages

import akka.util.ByteString

object Header {

  def Length = 4

  def apply(version : Byte, opCode : Byte, stream : Byte) : Header = {
    new Header(version, opCode, stream)
  }
}

class Header(val version : Int, val opCode : Byte, val streamId : Byte, val flags : Byte = 0x0) {

  def serialize() : Array[Byte] = {
    val bs = ByteString.newBuilder

    bs.putByte(versionAsByte())
    bs.putByte(flags.toByte)
    bs.putByte(streamId)
    bs.putByte(opCode.toByte)

    bs.result().toArray
  }

  def versionAsByte() : Byte = {
    (version & 0xFF).toByte
  }

}
