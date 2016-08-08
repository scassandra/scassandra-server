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
package org.scassandra.codec

import java.net.{InetAddress, InetSocketAddress, UnknownHostException}

import org.scassandra.codec.datatype.DataType
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

sealed trait Value
case object Null extends Value
case object Unset extends Value
case class Bytes(bytes: ByteVector) extends Value

case class QueryValue(name: Option[String], value: Value)

object QueryValue {
  def value(value: Any, dataType: DataType)(implicit protocolVersion: ProtocolVersion) = QueryValue(None, Bytes(dataType.codec.encode(value).require.bytes))
}

object ValueCodec extends Codec[Value] {
  import Notations.int

  override def decode(bits: BitVector): Attempt[DecodeResult[Value]] = {
    int.decode(bits) match {
      case Successful(DecodeResult(count, remainder)) => count match {
        case -1          => Successful(DecodeResult(Null, remainder))
        case -2          => Successful(DecodeResult(Unset, remainder))
        case i if i >= 0 => variableSizeBytes(provide(i), bytes).decode(remainder).map(_.map(Bytes))
        case _           => Failure(Err(s"Invalid [value] count $count"))
      }
      case f: Failure => f
    }
  }

  override def encode(value: Value): Attempt[BitVector] = value match {
    case Null     => int.encode(-1)
    case Unset    => int.encode(-2)
    case Bytes(b) => int.encode(b.length.toInt).map(_ ++ b.toBitVector)
  }

  override def sizeBound: SizeBound = int.sizeBound.atLeast
}

private[codec] object InetAddressCodec extends Codec[InetSocketAddress] {
  import Notations.int

  override def decode(bits: BitVector): Attempt[DecodeResult[InetSocketAddress]] = {
    variableSizeBytes(uint8, bytes).decode(bits) match {
      case Successful(DecodeResult(bytes, remainder)) =>
        try {
          val address = InetAddress.getByAddress(bytes.toArray)
          int.decode(remainder) match {
            case Successful(DecodeResult(port, r)) => Successful(DecodeResult(new InetSocketAddress(address, port), r))
            case f: Failure => f
          }
        } catch {
          // handle case where given an invalid address by wrapping UHE exception in a failure.
          case u: UnknownHostException => Failure(Err(u.getMessage))
        }
      case f: Failure => f
    }
  }

  override def encode(value: InetSocketAddress): Attempt[BitVector] = {
    val address = value.getAddress.getAddress
    for {
      size <- uint8.encode(address.length.toByte)
      bytes = BitVector(address)
      port <- int.encode(value.getPort)
    } yield size ++ bytes ++ port
  }

  override def sizeBound: SizeBound = {
    // base is the size of the address size + size of the port.
    val baseSize = uint8.sizeBound.lowerBound + int.sizeBound.lowerBound
    // min would be ipv4, max ipv6.
    SizeBound(baseSize + 4, Some(baseSize + 16))
  }
}

object Consistency extends Enumeration {
  type Consistency = Value
  val ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE = Value

  val all = Consistency.values.iterator.toList

  implicit val codec: Codec[Consistency] = enumerated(Notations.short, Consistency)
}

object Notations {
  val byte = scodec.codecs.byte
  val int = int32
  val long = int64
  val short = uint16
  val string = variableSizeBytes(short, utf8)
  val longString = variableSizeBytes(int, utf8)
  val uuid = scodec.codecs.uuid
  val stringList = listOfN(short, string)
  val bytes = variableSizeBytes(int, scodec.codecs.bytes)
  val value: Codec[Value] = ValueCodec
  val shortBytes = variableSizeBytes(short, scodec.codecs.bytes)
  def option[B](map: Map[B,Int]) = mappedEnum(short, map)
  val inet: Codec[InetSocketAddress] = InetAddressCodec
  val consistency = Consistency.codec
  val stringMap = map(short, string, string)
  val stringMultimap = map(short, string, stringList)
  val bytesMap = map(short, string, bytes)

  val queryValueWithName = (conditional(true, cstring) :: value).as[QueryValue]
  val queryValueWithoutName = (conditional(false, cstring) :: value).as[QueryValue]
  def queryValue(includeName: Boolean) = if(includeName) queryValueWithName else queryValueWithoutName

  def map[K,V](countCodec: Codec[Int], keyCodec: Codec[K], valCodec: Codec[V]): Codec[Map[K,V]] =
    listOfN(countCodec, (keyCodec :: valCodec).as[(K, V)]).xmap[Map[K,V]](_.toMap, _.toList)
}