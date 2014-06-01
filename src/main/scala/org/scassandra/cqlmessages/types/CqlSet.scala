package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

// only supports strings for now.
case class CqlSet(setType : ColumnType[_]) extends ColumnType[Option[Set[_]]](0x0022, s"set<${setType.stringRep}>") {
   override def readValue(byteIterator: ByteIterator): Option[Set[String]] = {
     CqlProtocolHelper.readVarcharSetValue(byteIterator)
   }

   def writeValue(value: Any) = {
     if (value.isInstanceOf[Set[_]] || value.isInstanceOf[Seq[_]]) {
       CqlProtocolHelper.serializeSet(value.asInstanceOf[Iterable[setType.type]], setType)
     } else {
       throw new IllegalArgumentException(s"Can't serialise ${value} as Set of ${setType}")
     }
   }
 }
