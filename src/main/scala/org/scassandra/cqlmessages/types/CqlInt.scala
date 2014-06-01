package org.scassandra.cqlmessages.types

import akka.util.{ByteString, ByteIterator}
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlInt extends ColumnType[Option[Int]](0x0009, "int") {

  import CqlProtocolHelper._

  override def readValue(byteIterator: ByteIterator): Option[Int] = {
    CqlProtocolHelper.readIntValue(byteIterator)
  }

  def writeValue(value: Any) = {
    val bs = ByteString.newBuilder
    bs.putInt(4)
    val valueAsInt = value match {
      case bd: BigDecimal => {
        if (bd.isValidInt) {
          bd.toInt
        } else {
          throw new IllegalArgumentException
        }
      }
      case asString: String => asString.toInt
      case unknownType@_ => throw new IllegalArgumentException(s"Can't serialise ${value} with type ${value.getClass} as Int")
    }
    bs.putInt(valueAsInt)
    bs.result().toArray
  }
}
