package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlFloat extends ColumnType[Option[Float]](0x0008, "float") {
   override def readValue(byteIterator: ByteIterator): Option[Float] = {
     CqlProtocolHelper.readFloatValue(byteIterator)
   }

   def writeValue(value: Any) = {
     CqlProtocolHelper.serializeFloatValue(value.toString.toFloat)
   }
 }
