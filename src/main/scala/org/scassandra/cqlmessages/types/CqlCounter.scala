package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlCounter extends ColumnType[Option[Long]](0x0005, "counter") {
   override def readValue(byteIterator: ByteIterator): Option[Long] = {
     CqlProtocolHelper.readBigIntValue(byteIterator)
   }

   def writeValue(value: Any) = {
     CqlProtocolHelper.serializeBigIntValue(value.toString.toLong)
   }
 }
