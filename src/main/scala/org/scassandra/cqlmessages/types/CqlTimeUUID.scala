package org.scassandra.cqlmessages.types

import java.util.UUID
import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlTimeUUID extends ColumnType[Option[UUID]](0x000F, "timeuuid") {
   override def readValue(byteIterator: ByteIterator): Option[UUID] = {
     CqlProtocolHelper.readUUIDValue(byteIterator)
   }

   def writeValue( value: Any) = {
     CqlProtocolHelper.serializeUUIDValue(UUID.fromString(value.toString))
   }
 }
