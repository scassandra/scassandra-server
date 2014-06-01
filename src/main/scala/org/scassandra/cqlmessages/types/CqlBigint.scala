package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlBigint extends ColumnType[Option[Long]](0x0002, "bigint") {
   override def readValue(byteIterator: ByteIterator) = {
     CqlProtocolHelper.readBigIntValue(byteIterator)
   }

   override def writeValue(value: Any) = {
     CqlProtocolHelper.serializeBigIntValue(value.toString.toLong)
   }
 }
