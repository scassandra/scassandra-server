/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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
package org.scassandra.codec.datatype

import java.net.{InetAddress, UnknownHostException}
import java.util.UUID

import org.scassandra.codec.Notations.{map, int => cint, short => cshort}
import org.scassandra.codec._
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Codec, DecodeResult, Err, SizeBound}

sealed trait DataType {
  import DataType.AnyCodecDecorators

  val native: PartialFunction[Any, Any]
  def baseCodec(implicit protocolVersion: ProtocolVersion): Codec[_]
  def codec(implicit protocolVersion: ProtocolVersion): Codec[Any] = baseCodec.asInstanceOf[Codec[Any]].withAny(native)
  val stringRep: String
}

object DataType {

  sealed trait PrimitiveType extends DataType

  implicit class AnyCodecDecorators(codec: Codec[Any]) {
    def withAny(f: PartialFunction[Any, Any]): Codec[Any] = {
      codec.widen(
        x => x,
        (a: Any) => {
          if (f.isDefinedAt(a)) {
            try {
              val res = f(a)
              Successful(res)
            } catch {
              case(e: Throwable) => Failure(Err(s"${e.getClass.getName}(${e.getMessage})"))
            }
          } else {
            Failure(Err(s"Unsure how to encode $a"))
          }
        }
      )
    }
  }

  // TODO: Could use macro instead to resolve all the primitive types instead of keeping a separate list.
  private [this] lazy val primitiveTypes = scala.List(
    Ascii, Bigint, Blob, Boolean, Counter, Decimal, Double, Float, Int, Text, Timestamp, Uuid, Varchar, Varint,
    Timeuuid, Inet, Date, Time, Smallint, Tinyint
  )

  lazy val primitiveTypeMap = {
    primitiveTypes.map(t => (t.stringRep, t)).toMap
  }

  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[DataType] = protocolVersion.dataTypeCodec

  case class Custom(className: String) extends DataType {
    val stringRep = s"'$className'"

    val native: PartialFunction[Any, Any] = {
      case b: Array[Byte] => ByteVector(b)
      case b: ByteVector => b
      case b: BitVector => b.bytes
      case s: String => ByteVector.fromValidHex(s.toLowerCase)
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = bytes
  }

  case object Ascii extends PrimitiveType {
    val stringRep = "ascii"

    val native: PartialFunction[Any, Any] = {
      case s: String => s
      case x: Any if !x.isInstanceOf[Iterable[_]] && !x.isInstanceOf[scala.Predef.Map[_,_]] => x.toString
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = ascii
  }

  case object Bigint extends PrimitiveType {
    val stringRep = "bigint"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toLong
      case x: Number => x.longValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = int64
  }

  case object Blob extends PrimitiveType {
    val stringRep = "blob"

    val native: PartialFunction[Any, Any] = {
      case b: Array[Byte] => ByteVector(b)
      case b: ByteVector => b
      case b: BitVector => b.bytes
      // TODO: What should we do in the case where we can't get hex from string?
      case s: String => ByteVector.fromValidHex(s.toLowerCase)
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = bytes
  }

  case object Boolean extends PrimitiveType {
    val stringRep = "boolean"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toBoolean
      case x: Number => x.toString.toBoolean
      case b: Boolean => b
    }

    // a custom codec is used in favor of bool(8) because we want true to be represented as 0x01 instead of 0xFF
    def baseCodec(implicit protocolVersion: ProtocolVersion) = new Codec[Boolean] {
      private val zero = BitVector.lowByte
      private val one = BitVector.fromByte(1)
      private val codec = bits(8).xmap[Boolean](bits => !(bits == zero), b => if (b) one else zero)
      def sizeBound = SizeBound.exact(8)
      def encode(b: Boolean) = codec.encode(b)
      def decode(b: BitVector) = codec.decode(b)
      override def toString = s"boolean"
    }
  }

  case object Counter extends PrimitiveType {
    val stringRep = "counter"

    val native = Bigint.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Bigint.baseCodec
  }

  case object Decimal extends PrimitiveType {
    val stringRep = "decimal"

    val native: PartialFunction[Any, Any] = {
      case s: String => BigDecimal(s)
      case b: BigDecimal => b
      case x: Number => BigDecimal(x.doubleValue())
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = cint.pairedWith(Varint.baseCodec).exmap(
      (t: (Int, BigInt)) => Successful(BigDecimal(t._2, t._1)),
      (b: BigDecimal)    => Successful((b.scale, BigInt(b.bigDecimal.unscaledValue())))
    )
  }

  case object Double extends PrimitiveType {
    val stringRep = "double"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toDouble
      case x: Number => x.doubleValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = double
  }

  case object Float extends PrimitiveType {
    val stringRep = "float"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toFloat
      case x: Number => x.floatValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = float
  }

  case object Int extends PrimitiveType {
    val stringRep = "int"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toInt
      case x: Number => x.intValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = int32
  }

  case object Timestamp extends PrimitiveType {
    val stringRep = "timestamp"

    val native = Bigint.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Bigint.baseCodec
  }

  case object Uuid extends PrimitiveType {
    val stringRep = "uuid"

    val native: PartialFunction[Any, Any] = {
      case s: String => UUID.fromString(s)
      case u: UUID => u
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = uuid
  }

  case object Varchar extends PrimitiveType {
    val stringRep = "varchar"

    val native: PartialFunction[Any, Any] = {
      case s: String => s
      case x: Any if !x.isInstanceOf[Iterable[_]] && !x.isInstanceOf[scala.Predef.Map[_,_]] => x.toString
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = utf8
  }

  case object Text extends PrimitiveType {
    val stringRep = "text"

    val native: PartialFunction[Any, Any] = {
      case s: String => s
      case x: Any if !x.isInstanceOf[Iterable[_]] && !x.isInstanceOf[scala.Predef.Map[_,_]] => x.toString
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = utf8
  }

  case object Varint extends PrimitiveType {
    val stringRep = "varint"

    val native: PartialFunction[Any, Any] = {
      case s: String => BigInt(s)
      case b: BigInt => b
      case x: Number => BigInt(x.longValue())
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Codec(
      (b: BigInt) => Successful(BitVector(b.toByteArray)),
      (b: BitVector) => Successful(DecodeResult(BigInt(b.toByteArray), BitVector.empty))
    )
  }

  case object Timeuuid extends PrimitiveType {
    val stringRep = "timeuuid"

    val native = Uuid.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Uuid.baseCodec
  }

  case object Inet extends PrimitiveType {
    val stringRep = "inet"

    val native: PartialFunction[Any, Any] = {
      // TODO: This will do a DNS lookup if a non-ip address is provided.
      // Should probably not allow this.
      case s: String => InetAddress.getByName(s)
      case a: InetAddress => a
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Codec(
      (i: InetAddress) => Successful(BitVector(i.getAddress)),
      (b: BitVector) => try {
        Successful(DecodeResult(InetAddress.getByAddress(b.toByteArray), BitVector.empty))
      } catch {
        case e: UnknownHostException => Failure(Err(e.getMessage))
      }
    )
  }

  case object Date extends PrimitiveType {
    val stringRep = "date"

    val native: PartialFunction[Any, Any] = {
      // TODO: perhaps parse literal dates, i.e. 1985-01-01
      case s: String => s.toLong
      case x: Number => x.longValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = uint32
  }

  case object Time extends PrimitiveType {
    val stringRep = "time"

    val native: PartialFunction[Any, Any] = Bigint.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Bigint.baseCodec.widen(
      (in: Long) => in,
      (in: Long) => in match {
        case x if x >= 0 && x <= 86399999999999L => Successful(x)
        case x => Failure(Err(s"$x is not between 0 and 86399999999999"))
      }
    )
  }

  case object Smallint extends PrimitiveType {
    val stringRep = "smallint"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toShort
      case x: Number => x.shortValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = short16
  }

  case object Tinyint extends PrimitiveType {
    val stringRep = "tinyint"

    val native: PartialFunction[Any, Any] = {
      case s: String => s.toByte
      case x: Number => x.byteValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = byte
  }

  case class List(element: DataType) extends DataType {
    val stringRep = s"list<${element.stringRep}>"

    val native: PartialFunction[Any, Any] = {
      case t: TraversableOnce[_] => t.toList
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = listOfN(protocolVersion.collectionLengthCodec,
      variableSizeBytes(protocolVersion.collectionLengthCodec, element.codec))
  }

  case class Map(key: DataType, value: DataType) extends DataType {
    val stringRep = s"map<${key.stringRep},${value.stringRep}>"

    val native: PartialFunction[Any, Any] = {
      case m: scala.Predef.Map[_, _] => m
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = map(protocolVersion.collectionLengthCodec,
      variableSizeBytes(protocolVersion.collectionLengthCodec, key.codec),
      variableSizeBytes(protocolVersion.collectionLengthCodec, value.codec))
  }

  case class Set(element: DataType) extends DataType {
    val stringRep = s"set<${element.stringRep}>"

    val native: PartialFunction[Any, Any] = {
      case t: TraversableOnce[_] => t.toSet
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = listOfN(protocolVersion.collectionLengthCodec,
      variableSizeBytes(protocolVersion.collectionLengthCodec, element.codec)).xmap(
      (f: scala.List[Any]) => f.toSet,
      (f: scala.Predef.Set[Any]) => f.toList
    )
  }

  case class Tuple(elements: scala.List[DataType]) extends DataType {

    val stringRep = s"tuple<${elements.map(_.stringRep).mkString(",")}>"

    val native: PartialFunction[Any, Any] = {
      case a: TraversableOnce[_] => a.toList
      // Allows encoding of any TupleXX
      case p: Product => p.productIterator.toList
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = TupleCodec(this)
  }

  object Tuple {
    def apply(elements: DataType*): Tuple = Tuple(elements.toList)
  }
}




