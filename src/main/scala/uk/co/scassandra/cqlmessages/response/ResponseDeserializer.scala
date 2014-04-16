package org.scassandra.cqlmessages.response

import akka.util.{ByteIterator, ByteString}
import org.scassandra.cqlmessages._
import com.typesafe.scalalogging.slf4j.Logging
import java.nio.ByteOrder

object ResponseDeserializer extends Logging {

  implicit val byteOrder : ByteOrder = ByteOrder.BIG_ENDIAN

  val HeaderLength = 8

  def deserialize(byteString: ByteString): Response = {

    if (byteString.length < HeaderLength) throw new IllegalArgumentException

    val iterator = byteString.iterator
    val protocolVersion = iterator.getByte
    val flags = iterator.getByte
    val stream = iterator.getByte
    val opCode = iterator.getByte
    val bodyLength = iterator.getInt

    if (iterator.len < bodyLength) throw new IllegalArgumentException

    opCode  match {
      case OpCodes.Ready => new Ready
      case OpCodes.Result => Result.fromByteString(byteString)
      case opCode @ _ =>
        throw new IllegalArgumentException(s"Received unknown opcode ${opCode}")
    }
  }

  def readString(iterator: ByteIterator, length : Int) = {
    val bytes = new Array[Byte](length)
    iterator.getBytes(bytes)
    new String(bytes)
  }
}
