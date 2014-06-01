package org.scassandra.cqlmessages.types

import akka.util.ByteIterator

case object CqlText extends ColumnType[Option[String]](0x000A, "text") {
   override def readValue(byteIterator: ByteIterator): Option[String] = {
     CqlVarchar.readValue(byteIterator)
   }

   override def writeValue(value : Any) = {
     CqlVarchar.writeValue(value)
   }

   override def writeValueInCollection(value : Any) = {
     CqlVarchar.writeValueInCollection(value)
   }
 }
