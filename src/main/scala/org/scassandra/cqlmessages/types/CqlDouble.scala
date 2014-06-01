package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlDouble extends ColumnType[Option[Double]](0x0007, "double") {
   override def readValue(byteIterator: ByteIterator): Option[Double] = {
     CqlProtocolHelper.readDoubleValue(byteIterator)
   }

   def writeValue( value: Any) = {
     CqlProtocolHelper.serializeDoubleValue(value.toString.toDouble)
   }
 }
