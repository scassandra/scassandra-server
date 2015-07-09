package org.scassandra.server.cqlmessages.types

import java.util.UUID

import akka.util.{ByteString, ByteIterator}
import org.scassandra.server.cqlmessages.ProtocolVersion

abstract class CqlUUIDType(override val code : Short, override val stringRep: String) extends ColumnType[UUID](code: Short, stringRep: String) {

  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[UUID] = {
    val mostSignificantBytes = byteIterator.getLong
    val leastSignificantBytes = byteIterator.getLong
    Some(new UUID(mostSignificantBytes, leastSignificantBytes))
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion) = {
    val uuid = UUID.fromString(value.toString)
    val bs = ByteString.newBuilder
    bs.putLong(uuid.getMostSignificantBits)
    bs.putLong(uuid.getLeastSignificantBits)
    bs.result().toArray
  }
}