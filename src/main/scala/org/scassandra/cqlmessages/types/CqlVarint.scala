package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlVarint extends ColumnType[Option[BigInt]](0x000E, "varint") {
   override def readValue(byteIterator: ByteIterator): Option[BigInt] = {
     CqlProtocolHelper.readVarintValue(byteIterator)
   }

   def writeValue( value: Any) = {
     CqlProtocolHelper.serializeVarintValue(BigInt(value.toString))
   }
 }
