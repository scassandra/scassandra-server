package org.scassandra.server.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.server.cqlmessages.ProtocolVersion

abstract class CqlTextType(override val code : Short, override val stringRep: String, val charSet: String = "UTF-8") extends ColumnType[String](code: Short, stringRep: String) {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[String] = {
    Some(byteIterator.toByteString.decodeString(charSet))
  }

  override def writeValue(value : Any)(implicit protocolVersion: ProtocolVersion) = {
    if (value.isInstanceOf[Iterable[_]] || value.isInstanceOf[Map[_,_]]) throw new IllegalArgumentException(s"Can't serialise ${value} as String")

    value.toString.getBytes(charSet)
  }
}
