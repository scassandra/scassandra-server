package org.scassandra.cqlmessages.types

import akka.util.ByteIterator

case object CqlAscii extends ColumnType[Option[String]](0x0001, "ascii") {
   override def readValue(byteBuffer : ByteIterator): Option[String] = {
     CqlVarchar.readValue(byteBuffer)
   }

   def writeValue(value : Any) = {
     CqlVarchar.writeValue(value)
   }

   override def writeValueInCollection(value: Any) : Array[Byte] = {
     CqlVarchar.writeValueInCollection(value)
   }
 }
