package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlDecimal extends ColumnType[Option[BigDecimal]](0x0006, "decimal") {
   override def readValue(byteIterator: ByteIterator): Option[BigDecimal] = {
     CqlProtocolHelper.readDecimalValue(byteIterator)
   }

   def writeValue(value: Any) = {
     CqlProtocolHelper.serializeDecimalValue(new java.math.BigDecimal(value.toString))
   }
 }
