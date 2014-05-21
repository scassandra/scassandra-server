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

import akka.util.ByteIterator
import java.util.UUID
import java.net.InetAddress

abstract class ColumnType[T](val code : Short, val stringRep: String) {
  def readValue(byteIterator : ByteIterator) : T
}

case object CqlAscii extends ColumnType[String](0x0001, "ascii") {
  override def readValue(byteBuffer : ByteIterator): String = {
    CqlProtocolHelper.readLongString(byteBuffer)  
  }
}
case object CqlBigint extends ColumnType[Long](0x0002, "bigint") {
  override def readValue(byteIterator: ByteIterator): Long = {
    CqlProtocolHelper.readBigIntValue(byteIterator)
  }
} 
case object CqlBlob extends ColumnType[Array[Byte]](0x0003, "blob") {
  override def readValue(byteIterator: ByteIterator): Array[Byte] = {
    CqlProtocolHelper.readBlobValue(byteIterator)
  }
}
case object CqlBoolean extends ColumnType[Boolean](0x0004, "boolean") {
  override def readValue(byteIterator: ByteIterator): Boolean = {
    CqlProtocolHelper.readBooleanValue(byteIterator)
  }
}
case object CqlCounter extends ColumnType[Long](0x0005, "counter") {
  override def readValue(byteIterator: ByteIterator): Long = {
    CqlProtocolHelper.readBigIntValue(byteIterator)
  }
}
case object CqlDecimal extends ColumnType[BigDecimal](0x0006, "decimal") {
  override def readValue(byteIterator: ByteIterator): BigDecimal = {
    CqlProtocolHelper.readDecimalValue(byteIterator)
  }
}
case object CqlDouble extends ColumnType[Double](0x0007, "double") {
  override def readValue(byteIterator: ByteIterator): Double = {
    CqlProtocolHelper.readDoubleValue(byteIterator)
  }
}
case object CqlFloat extends ColumnType[Float](0x0008, "float") {
  override def readValue(byteIterator: ByteIterator): Float = {
    CqlProtocolHelper.readFloatValue(byteIterator)
  }
}
case object CqlInt extends ColumnType[Int](0x0009, "int") {
  override def readValue(byteIterator: ByteIterator): Int = {
    CqlProtocolHelper.readIntValue(byteIterator)
  }
}
case object CqlText extends ColumnType[String](0x000A, "text") {
  override def readValue(byteIterator: ByteIterator): String = {
    CqlProtocolHelper.readLongString(byteIterator)
  }
}
case object CqlTimestamp extends ColumnType[Long](0x000B, "timestamp") {
  override def readValue(byteIterator: ByteIterator): Long = {
    CqlProtocolHelper.readTimestampValue(byteIterator)
  }
}
case object CqlUUID extends ColumnType[UUID](0x000C, "uuid") {
  override def readValue(byteIterator: ByteIterator): UUID = {
    CqlProtocolHelper.readUUIDValue(byteIterator)
  }

}
case object CqlVarchar extends ColumnType[String](0x000D, "varchar") {
  override def readValue(byteIterator: ByteIterator): String = {
    CqlProtocolHelper.readLongString(byteIterator)
  }
}
case object CqlVarint extends ColumnType[BigInt](0x000E, "varint") {
  override def readValue(byteIterator: ByteIterator): BigInt = {
    CqlProtocolHelper.readVarintValue(byteIterator)
  }
}
case object CqlTimeUUID extends ColumnType[UUID](0x000F, "timeuuid") {
  override def readValue(byteIterator: ByteIterator): UUID = {
    CqlProtocolHelper.readUUIDValue(byteIterator)
  }
}
case object CqlInet extends ColumnType[InetAddress](0x0010, "inet") {
  override def readValue(byteIterator: ByteIterator): InetAddress = {
    CqlProtocolHelper.readInetValue(byteIterator)
  }
}
// only supports strings for now.
case object CqlSet extends ColumnType[Set[String]](0x0022, "set") {
  override def readValue(byteIterator: ByteIterator): Set[String] = {
    CqlProtocolHelper.readVarcharSetValue(byteIterator)
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
    CqlSet.stringRep -> CqlSet,
    CqlVarchar.stringRep -> CqlVarchar
  )

  def fromString(string: String) : Option[ColumnType[_]] = {
    ColumnTypeMapping.get(string.toLowerCase())
  }
}
