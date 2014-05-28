/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.cqlmessages

import akka.util.{ByteString, ByteStringBuilder, ByteIterator}
import java.util.UUID
import java.net.InetAddress
import CqlProtocolHelper._
import com.typesafe.scalalogging.slf4j.{Logging}

abstract class ColumnType[T](val code : Short, val stringRep: String) extends Logging {
  def readValue(byteIterator : ByteIterator) : T
  def writeValue(value : Any) : Array[Byte]
  def writeValueInCollection(value: Any) : Array[Byte] = ???
}

case object CqlAscii extends ColumnType[String](0x0001, "ascii") {
  override def readValue(byteBuffer : ByteIterator): String = {
    readLongString(byteBuffer)  
  }

  def writeValue( value : Any) = {
    CqlProtocolHelper.serializeLongString(value.toString)
  }
}
case object CqlBigint extends ColumnType[Long](0x0002, "bigint") {
  override def readValue(byteIterator: ByteIterator): Long = {
    CqlProtocolHelper.readBigIntValue(byteIterator)
  }

  override def writeValue(value: Any) = {
    CqlProtocolHelper.serializeBigIntValue(value.toString.toLong)
  }
} 
case object CqlBlob extends ColumnType[Array[Byte]](0x0003, "blob") {
  override def readValue(byteIterator: ByteIterator): Array[Byte] = {
    CqlProtocolHelper.readBlobValue(byteIterator)
  }

  override def writeValue(value: Any) = {
    val bs = ByteString.newBuilder
    val array = hex2Bytes(value.toString)
    bs.putInt(array.length)
    bs.putBytes(array)
    bs.result().toArray
  }

  private def hex2Bytes(hex: String): Array[Byte] = {
    try {
      (for {i <- 0 to hex.length - 1 by 2 if i > 0 || !hex.startsWith("0x")}
      yield hex.substring(i, i + 2))
        .map(hexValue => Integer.parseInt(hexValue, 16).toByte).toArray
    }
    catch {
      case s : Exception => throw new IllegalArgumentException(s"Not valid hex $hex")
    }
  }
}
case object CqlBoolean extends ColumnType[Boolean](0x0004, "boolean") {
  override def readValue(byteIterator: ByteIterator): Boolean = {
    CqlProtocolHelper.readBooleanValue(byteIterator)
  }

  def writeValue(value: Any) = {
    CqlProtocolHelper.serializeBooleanValue(value.toString.toBoolean)
  }
}
case object CqlCounter extends ColumnType[Long](0x0005, "counter") {
  override def readValue(byteIterator: ByteIterator): Long = {
    CqlProtocolHelper.readBigIntValue(byteIterator)
  }

  def writeValue(value: Any) = {
    CqlProtocolHelper.serializeBigIntValue(value.toString.toLong)
  }
}
case object CqlDecimal extends ColumnType[BigDecimal](0x0006, "decimal") {
  override def readValue(byteIterator: ByteIterator): BigDecimal = {
    CqlProtocolHelper.readDecimalValue(byteIterator)
  }

  def writeValue(value: Any) = {
    CqlProtocolHelper.serializeDecimalValue(new java.math.BigDecimal(value.toString))
  }
}
case object CqlDouble extends ColumnType[Double](0x0007, "double") {
  override def readValue(byteIterator: ByteIterator): Double = {
    CqlProtocolHelper.readDoubleValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeDoubleValue(value.toString.toDouble)
  }
}
case object CqlFloat extends ColumnType[Float](0x0008, "float") {
  override def readValue(byteIterator: ByteIterator): Float = {
    CqlProtocolHelper.readFloatValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeFloatValue(value.toString.toFloat)
  }
}
case object CqlInt extends ColumnType[Int](0x0009, "int") {
  override def readValue(byteIterator: ByteIterator): Int = {
    CqlProtocolHelper.readIntValue(byteIterator)
  }

  def writeValue(value: Any) = {
    val bs = ByteString.newBuilder
    bs.putInt(4)
    val valueAsInt = value match {
      case bd : BigDecimal => {
          if (bd.isValidInt) {
            bd.toInt
          } else {
            throw new IllegalArgumentException
          }
      }
      case asString : String => asString.toInt
      case unknownType @ _ => throw new IllegalArgumentException(s"Can't serialise ${value} with type ${value.getClass} as Int")
    }
    bs.putInt(valueAsInt)
    bs.result().toArray
  }
}
case object CqlText extends ColumnType[String](0x000A, "text") {
  override def readValue(byteIterator: ByteIterator): String = {
    CqlProtocolHelper.readLongString(byteIterator)
  }
  override def writeValue(value : Any) = {
    CqlProtocolHelper.serializeLongString(value.toString)
  }
}
case object CqlTimestamp extends ColumnType[Long](0x000B, "timestamp") {
  override def readValue(byteIterator: ByteIterator): Long = {
    CqlProtocolHelper.readTimestampValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeTimestampValue(value.toString.toLong)
  }
}
case object CqlUUID extends ColumnType[UUID](0x000C, "uuid") {
  override def readValue(byteIterator: ByteIterator): UUID = {
    CqlProtocolHelper.readUUIDValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeUUIDValue(UUID.fromString(value.toString))
  }
}
case object CqlVarchar extends ColumnType[String](0x000D, "varchar") {
  override def readValue(byteIterator: ByteIterator): String = {
    CqlProtocolHelper.readLongString(byteIterator)
  }
  override def writeValue(value : Any) = {
    CqlProtocolHelper.serializeLongString(value.toString)
  }
  override def writeValueInCollection(value : Any) = {
    CqlProtocolHelper.serializeString(value.toString)
  }
}
case object CqlVarint extends ColumnType[BigInt](0x000E, "varint") {
  override def readValue(byteIterator: ByteIterator): BigInt = {
    CqlProtocolHelper.readVarintValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeVarintValue(BigInt(value.toString))
  }
}
case object CqlTimeUUID extends ColumnType[UUID](0x000F, "timeuuid") {
  override def readValue(byteIterator: ByteIterator): UUID = {
    CqlProtocolHelper.readUUIDValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeUUIDValue(UUID.fromString(value.toString))
  }
}
case object CqlInet extends ColumnType[InetAddress](0x0010, "inet") {
  override def readValue(byteIterator: ByteIterator): InetAddress = {
    CqlProtocolHelper.readInetValue(byteIterator)
  }

  def writeValue( value: Any) = {
    CqlProtocolHelper.serializeInetValue(InetAddress.getByName(value.toString))
  }
}
// only supports strings for now.
case class CqlSet(setType : ColumnType[_]) extends ColumnType[Set[_]](0x0022, "set") {
  override def readValue(byteIterator: ByteIterator): Set[String] = {
    CqlProtocolHelper.readVarcharSetValue(byteIterator)
  }

  def writeValue(value: Any) = {
    if (value.isInstanceOf[Iterable[_]]) {
      CqlProtocolHelper.serializeSet(value.asInstanceOf[Iterable[setType.type]], setType)
    } else {
      throw new IllegalArgumentException(s"Can't serialise ${value} as Set of ${setType}")
    }
  }
}
case class CqlList(setType : ColumnType[_]) extends ColumnType[Iterable[_]](0x0020, "list") {
  override def readValue(byteIterator: ByteIterator): Iterable[String] = {
    CqlProtocolHelper.readVarcharSetValue(byteIterator)
  }

  def writeValue(value: Any) = {
    if (value.isInstanceOf[Iterable[_]]) {
      CqlProtocolHelper.serializeSet(value.asInstanceOf[Iterable[setType.type]], setType)
    } else {
      throw new IllegalArgumentException(s"Can't serialise ${value} as List of ${setType}")
    }
  }
}

object ColumnType {
  val ColumnTypeMapping = Map[String, ColumnType[_]](
    CqlInt.stringRep -> CqlInt,
    CqlBoolean.stringRep -> CqlBoolean,
    CqlAscii.stringRep -> CqlAscii,
    CqlBigint.stringRep -> CqlBigint,
    CqlCounter.stringRep -> CqlCounter,
    CqlBlob.stringRep -> CqlBlob,
    CqlDecimal.stringRep -> CqlDecimal,
    CqlDouble.stringRep -> CqlDouble,
    CqlFloat.stringRep -> CqlFloat,
    CqlText.stringRep -> CqlText,
    CqlTimestamp.stringRep -> CqlTimestamp,
    CqlUUID.stringRep -> CqlUUID,
    CqlInet.stringRep -> CqlInet,
    CqlVarint.stringRep -> CqlVarint,
    CqlTimeUUID.stringRep -> CqlTimeUUID,
    CqlVarchar.stringRep -> CqlVarchar,
    "set" -> CqlSet(CqlVarchar),
    "set<varchar>" -> CqlSet(CqlVarchar),
    "set<ascii>" -> CqlSet(CqlAscii),
    "set<text>" -> CqlSet(CqlText),
    "list" -> CqlList(CqlVarchar),
    "list<varchar>" -> CqlList(CqlVarchar),
    "list<ascii>" -> CqlList(CqlAscii),
    "list<text>" -> CqlList(CqlText)
  )

  def fromString(string: String) : Option[ColumnType[_]] = {
    ColumnTypeMapping.get(string.toLowerCase())
  }

}


