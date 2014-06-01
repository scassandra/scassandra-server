package org.scassandra.cqlmessages.types

import java.net.InetAddress
import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlInet extends ColumnType[Option[InetAddress]](0x0010, "inet") {
   override def readValue(byteIterator: ByteIterator): Option[InetAddress] = {
     CqlProtocolHelper.readInetValue(byteIterator)
   }

   def writeValue( value: Any) = {
     CqlProtocolHelper.serializeInetValue(InetAddress.getByName(value.toString))
   }
 }
