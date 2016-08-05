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
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Codec, DecodeResult, Err}

sealed trait DataType {
  val codec: Codec[Any]
  val stringRep: String
}

object DataType {

  sealed trait PrimitiveType extends DataType

  implicit class AnyCodecDecorators[T](codec: Codec[T]) {
    def withAny(f: PartialFunction[Any, T]): Codec[Any] = {
      codec.widen(
        x => x.asInstanceOf[Any],
        (a: Any) => {
          if (f.isDefinedAt(a)) {
            try {
              Successful(f(a))
            } catch {
              case(e: Throwable) => Failure(Err(s"${e.getClass.getName}(${e.getMessage})"))
            }
          } else {
            Failure(Err(s"Unsure how to decode $a"))
          }
        }
      )
    }
  }

  // TODO: Could use macro instead to resolve all the primitive types instead of keeping a separate list.
  private [this] val primitiveTypes = scala.List(
    Ascii, Bigint, Blob, Boolean, Counter, Decimal, Double, Float, Int, Text, Timestamp, Uuid, Varchar, Varint,
    Timeuuid, Inet, Date, Time, Smallint, Tinyint
  )

  lazy val primitiveTypeMap = {
    primitiveTypes.map(t => (t.stringRep, t)).toMap
  }

  implicit def codec: Codec[DataType] = discriminated[DataType].by(cshort)
    .typecase(0x00, cstring.as[Custom])
    .typecase(0x01, provide(Ascii))
    .typecase(0x02, provide(Bigint))
    .typecase(0x03, provide(Blob))
    .typecase(0x04, provide(Boolean))
    .typecase(0x05, provide(Counter))
    .typecase(0x06, provide(Decimal))
    .typecase(0x07, provide(Double))
    .typecase(0x08, provide(Float))
    .typecase(0x09, provide(Int))
    .typecase(0x0A, provide(Text))
    .typecase(0x0B, provide(Timestamp))
    .typecase(0x0C, provide(Uuid))
    .typecase(0x0D, provide(Varchar))
    .typecase(0x0E, provide(Varint))
    .typecase(0x0F, provide(Timeuuid))
    .typecase(0x10, provide(Inet))
    .typecase(0x11, provide(Date))
    .typecase(0x12, provide(Time))
    .typecase(0x13, provide(Smallint))
    .typecase(0x14, provide(Tinyint))
    .typecase(0x20, lazily(codec.as[List]))
    .typecase(0x21, lazily((codec :: codec).as[Map]))
    .typecase(0x22, lazily(codec.as[Set]))
    //.typecase(0x30, (cstring :: cstring :: cshort.consume(count => listOfN(provide(count), Codec[Field]))(_.size)).as[UDT])
    //.typecase(0x31, cshort.consume(count => listOfN(provide(count), codec))(_.size).as[Tuple])

  case class Custom(className: String) extends DataType {
    val stringRep = s"'$className'"
    val codec = bytes.withAny {
      case b: Array[Byte] => ByteVector(b)
      case b: ByteVector => b
      case b: BitVector => b.bytes
    }
  }

  case object Ascii extends PrimitiveType {
    val stringRep = "ascii"
    val codec = ascii.withAny {
      case s: String => s
      case x: Number => x.toString
    }
  }

  case object Bigint extends PrimitiveType {
    val stringRep = "bigint"
    val codec = int64.withAny {
      case s: String => s.toLong
      case x: Number => x.longValue()
    }
  }

  case object Blob extends PrimitiveType {
    val stringRep = "blob"
    val codec = bytes.withAny {
      // TODO: What should we do in the case where we can't get hex from string?
      case s: String => ByteVector.fromValidHex(s.toLowerCase)
    }
  }

  case object Boolean extends PrimitiveType {
    val stringRep = "boolean"
    val codec = bool(8).withAny {
      case s: String => s.toBoolean
      case x: Number => x.toString.toBoolean
      case b: Boolean => b
    }
  }

  case object Counter extends PrimitiveType {
    val stringRep = "counter"
    val codec = Bigint.codec
  }

  case object Decimal extends PrimitiveType {
    val stringRep = "decimal"
    val codec = cint.pairedWith(Varint.varintCodec).exmap(
      (t: (Int, BigInt)) => Successful(BigDecimal(t._2, t._1)),
      (b: BigDecimal)    => Successful((b.scale, BigInt(b.bigDecimal.unscaledValue())))
    ).withAny {
      case s: String => BigDecimal(s)
      case b: BigDecimal => b
      case x: Number => BigDecimal(x.doubleValue())
    }
  }

  case object Double extends PrimitiveType {
    val stringRep = "double"
    val codec = double.withAny {
      case s: String => s.toDouble
      case x: Number => x.doubleValue()
    }
  }

  case object Float extends PrimitiveType {
    val stringRep = "float"
    val codec = float.withAny {
      case s: String => s.toFloat
      case x: Number => x.floatValue()
    }
  }

  case object Int extends PrimitiveType {
    val stringRep = "int"
    val codec = int32.withAny {
      case s: String => s.toInt
      case x: Number => x.intValue()
    }
  }

  case object Timestamp extends PrimitiveType {
    val stringRep = "timestamp"
    val codec = Bigint.codec
  }

  case object Uuid extends PrimitiveType {
    val stringRep = "uuid"
    val codec = uuid.withAny {
      case s: String => UUID.fromString(s)
      case u: UUID => u
    }
  }

  case object Varchar extends PrimitiveType {
    val stringRep = "varchar"
    val codec = utf8.withAny {
      case s: String => s
      case x: Number => x.toString
    }
  }

  case object Text extends PrimitiveType {
    val stringRep = "text"
    val codec = utf8.withAny {
      case s: String => s
      case x: Number => x.toString
    }
  }

  case object Varint extends PrimitiveType {
    val stringRep = "varint"
    val varintCodec = Codec(
      (b: BigInt) => Successful(BitVector(b.toByteArray)),
      (b: BitVector) => Successful(DecodeResult(BigInt(b.toByteArray), BitVector.empty))
    )
    val codec = varintCodec.withAny {
      case s: String => BigInt(s)
      case b: BigInt => b
      case x: Number => BigInt(x.longValue())
    }
  }

  case object Timeuuid extends PrimitiveType {
    val stringRep = "timeuuid"
    val codec = Uuid.codec
  }

  case object Inet extends PrimitiveType {
    val stringRep = "inet"
    val codec = Codec(
      (i: InetAddress) => Successful(BitVector(i.getAddress)),
      (b: BitVector) => try {
        Successful(DecodeResult(InetAddress.getByAddress(b.toByteArray), BitVector.empty))
      } catch {
        case e: UnknownHostException => Failure(Err(e.getMessage))
      }
    ).withAny {
      // TODO avoid DNS lookup
      case s: String => InetAddress.getByName(s)
      case a: InetAddress => a
    }
  }

  case object Date extends PrimitiveType {
    val stringRep = "date"
    val codec = uint32.withAny {
      case s: String => s.toInt
      case x: Number => x.intValue()
    }
  }

  case object Time extends PrimitiveType {
    val stringRep = "time"
    val codec = Bigint.codec
  }

  case object Smallint extends PrimitiveType {
    val stringRep = "smallint"
    val codec = short16.withAny {
      case s: String => s.toShort
      case x: Number => x.shortValue()
    }
  }

  case object Tinyint extends PrimitiveType {
    val stringRep = "tinyint"
    val codec = byte.withAny {
      case s: String => s.toByte
      case x: Number => x.byteValue()
    }
  }

  case class List(element: DataType) extends DataType {
    val stringRep = s"list<${element.stringRep}>"
    // TODO: Set collection length based on protocol version.
    val codec = listOfN(cint, variableSizeBytes(cint, element.codec)).withAny {
      case t: TraversableOnce[_] => t.toList
    }
  }

  case class Map(key: DataType, value: DataType) extends DataType {
    val stringRep = s"map<${key.stringRep},${value.stringRep}>"
    val codec = map(cint, variableSizeBytes(cint, key.codec), variableSizeBytes(cint, value.codec)).withAny {
      case m: scala.Predef.Map[_, _] => m.asInstanceOf[scala.Predef.Map[Any, Any]]
    }
  }

  case class Set(element: DataType) extends DataType {
    val stringRep = s"set<${element.stringRep}>"
    val codec = listOfN(cint, variableSizeBytes(cint, element.codec)).xmap(
      (f: scala.List[Any]) => f.toSet,
      (f: scala.Predef.Set[Any]) => f.toList
    ).withAny {
      case t: TraversableOnce[_] => t.toSet
    }
  }


  /**
  case class UDT(keyspace: String, name: String, fields: scala.List[Field]) extends DataType {
  val codec = fields.map(_.dataType.codec).foldLeft[Codec[_]](ignore(0))((acc, c) => acc ~ c).withAny {
    case a: Any => a
  }
}
case class Tuple(elements: scala.List[DataType]) extends DataType {
  val codec = elements.map(_.codec).foldLeft[Codec[_]](ignore(0))((acc, c) => acc ~ c).withAny {
    case a: Any => a
  }
}*/

  case class Field(name: String, dataType: DataType)

  object Field {
    implicit val codec: Codec[Field] = (cstring :: DataType.codec).as[Field]
  }
}


