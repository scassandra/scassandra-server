package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlTimestamp extends ColumnType[Option[Long]](0x000B, "timestamp") {
  override def readValue(byteIterator: ByteIterator): Option[Long] = {
    CqlProtocolHelper.readTimestampValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeTimestampValue(value.toString.toLong)
  }
}
