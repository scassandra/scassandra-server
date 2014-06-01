package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case class CqlList(listType : ColumnType[_]) extends ColumnType[Option[Iterable[_]]](0x0020, s"list<${listType.stringRep}>") {
   override def readValue(byteIterator: ByteIterator): Option[Iterable[String]] = {
     CqlProtocolHelper.readVarcharSetValue(byteIterator)
   }

   def writeValue(value: Any) = {
     if (value.isInstanceOf[Set[_]] || value.isInstanceOf[Seq[_]]) {
       CqlProtocolHelper.serializeSet(value.asInstanceOf[Iterable[listType.type]], listType)
     } else {
       throw new IllegalArgumentException(s"Can't serialise ${value} as List of ${listType}")
     }
   }
 }
