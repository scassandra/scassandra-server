package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlVarchar extends ColumnType[Option[String]](0x000D, "varchar") {
   override def readValue(byteIterator: ByteIterator): Option[String] = {
     CqlProtocolHelper.readLongString(byteIterator)
   }
   override def writeValue(value : Any) = {
     if (value.isInstanceOf[Iterable[_]] || value.isInstanceOf[Map[_,_]]) throw new IllegalArgumentException(s"Can't serialise ${value} as String")
     CqlProtocolHelper.serializeLongString(value.toString)
   }
   override def writeValueInCollection(value : Any) = {
     CqlProtocolHelper.serializeString(value.toString)
   }
 }
