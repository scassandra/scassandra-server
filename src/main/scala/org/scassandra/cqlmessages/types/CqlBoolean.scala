package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlBoolean extends ColumnType[Option[Boolean]](0x0004, "boolean") {
   override def readValue(byteIterator: ByteIterator): Option[Boolean] = {
     CqlProtocolHelper.readBooleanValue(byteIterator)
   }

   def writeValue(value: Any) = {
     CqlProtocolHelper.serializeBooleanValue(value.toString.toBoolean)
   }
 }
