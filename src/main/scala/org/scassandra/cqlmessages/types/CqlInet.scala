package org.scassandra.cqlmessages.types

import java.net.InetAddress
import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper
import com.google.common.net.InetAddresses

case object CqlInet extends ColumnType[Option[InetAddress]](0x0010, "inet") {
  override def readValue(byteIterator: ByteIterator): Option[InetAddress] = {
    CqlProtocolHelper.readInetValue(byteIterator)
  }

  def writeValue(value: Any) = {
    CqlProtocolHelper.serializeInetValue(InetAddresses.forString(value.toString))
  }
}
