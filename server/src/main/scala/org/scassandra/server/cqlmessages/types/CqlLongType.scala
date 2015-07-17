package org.scassandra.server.cqlmessages.types

import java.lang

import akka.util.{ByteString, ByteIterator}
import org.scassandra.server.cqlmessages.ProtocolVersion

abstract class CqlLongType(override val code : Short, override val stringRep: String) extends ColumnType[lang.Long](code: Short, stringRep: String) {
  override def readValue(byteIterator: ByteIterator)(implicit protocolVersion: ProtocolVersion): Option[lang.Long] = {
    Some(byteIterator.getLong)
  }

  override def writeValue(value: Any)(implicit protocolVersion: ProtocolVersion) = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putLong(value.toString.toLong)
    frameBuilder.result().toArray
  }
}
