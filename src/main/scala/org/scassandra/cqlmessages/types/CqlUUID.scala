package org.scassandra.cqlmessages.types

import java.util.UUID
import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlUUID extends ColumnType[Option[UUID]](0x000C, "uuid") {
   override def readValue(byteIterator: ByteIterator): Option[UUID] = {
     CqlProtocolHelper.readUUIDValue(byteIterator)
   }

   def writeValue( value: Any) = {
     CqlProtocolHelper.serializeUUIDValue(UUID.fromString(value.toString))
   }
 }
