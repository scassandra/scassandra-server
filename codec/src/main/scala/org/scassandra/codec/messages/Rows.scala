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
package org.scassandra.codec.messages

import org.scassandra.codec.Notations.{value, bytes => cbytes, int => cint, string => cstring}
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Bytes, Null, Unset}
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, SizeBound}

import scala.collection.immutable
import scala.util.control.Breaks._

case class RowMetadata(
  flags: RowMetadataFlags = RowMetadataFlags(),
  pagingState: Option[ByteVector] = None,
  keyspace: Option[String] = None,
  table: Option[String] = None,
  columnSpec: Option[List[ColumnSpec]] = None
)

object NoRowMetadata extends RowMetadata(RowMetadataFlags())

object RowMetadata {
  implicit val codec: Codec[RowMetadata] = {
    ("flags"       | Codec[RowMetadataFlags]).flatPrepend { flags =>
    ("columnCount" | cint).consume { count =>
    ("pagingState" | conditional(flags.hasMorePages, cbytes))     ::
    ("keyspace"    | conditional(flags.globalTableSpec, cstring)) ::
    ("table"       | conditional(!flags.noMetadata && flags.globalTableSpec, cstring)) ::
    ("columnSpec"  | conditional(!flags.noMetadata, listOfN(provide(count), ColumnSpec.codec(!flags.globalTableSpec))))
  }(_.tail.tail.tail.head.getOrElse(Nil).size) // Extract column count from columnSpec, TODO: alternatively use _(3).getOrElse(Nil).size, it compiles but IDEs might not like it.
  }}.as[RowMetadata]
}

case class RowMetadataFlags(noMetadata: Boolean = false, hasMorePages: Boolean = false, globalTableSpec: Boolean = false)

object RowMetadataFlags {
  implicit val codec: Codec[RowMetadataFlags] = {
    ("reserved"        | ignore(29))::
    ("noMetadata"      | bool)      ::
    ("hasMorePages"    | bool)      ::
    ("globalTableSpec" | bool)
  }.dropUnits.as[RowMetadataFlags]
}

sealed trait ColumnSpec {
  val name: String
  val dataType: DataType
}

case class ColumnSpecWithoutTable(override val name: String, override val dataType: DataType) extends ColumnSpec
case class ColumnSpecWithTable(keyspace: String, table: String, override val name: String, override val dataType: DataType) extends ColumnSpec

object ColumnSpec {
  def codec(withTable: Boolean): Codec[ColumnSpec] = {
    if(withTable) {
      (cstring :: cstring :: cstring  :: Codec[DataType]).as[ColumnSpecWithTable].upcast[ColumnSpec]
    } else {
      (cstring :: Codec[DataType]).as[ColumnSpecWithoutTable].upcast[ColumnSpec]
    }
  }

  def column(name: String, dataType: DataType) = ColumnSpecWithoutTable(name, dataType)
}

case class Row(columns: Map[String, Any])

object Row {
  def withColumnSpec(spec: List[ColumnSpec]): Codec[Row] = RowCodec(spec)
  def apply(colPairs: (String, Any)*): Row = Row(colPairs.toMap)
}

case class RowCodec(columnSpecs: List[ColumnSpec]) extends Codec[Row] {

  private[this] lazy val columnsWithCodecs: List[(String, Codec[Any])] = columnSpecs.map { (spec: ColumnSpec) =>
    (spec.name, variableSizeBytes(cint, spec.dataType.codec))
  }

  override def decode(bits: BitVector): Attempt[DecodeResult[Row]] = {
    var remaining = bits
    val data = immutable.List.newBuilder[(String, _ <: Any)]
    var result: Option[Attempt[DecodeResult[Row]]] = None

    // for each column spec, attempt to decode value
    breakable {
      for (spec <- columnSpecs) {
        val codec = spec.dataType.codec
        // first parse using value codec to detect any possible Null/Unset values.
        value.decode(bits) match {
          case Successful(DecodeResult(v, r)) => {
            remaining = r
            val value: Option[Any] = v match {
              // Decode using actual codec.
              case Bytes(b) => codec.decode(b.bits) match {
                case Successful(DecodeResult(aV, _)) => Some(aV)
                case f: Failure => {
                  // Failure to decode, break out and set error.
                  result = Some(f)
                  break
                }
              }
              // Null value will store null in map.
              case Null => Some(null)
              // Unset value will do nothing.
              case Unset => None
            }
            // If there was a set value or null add it to map.
            value match {
              case Some(v) => data += ((spec.name, v))
              case None => ()
            }
          }
          // Failure to decode into bytes, it is unlikely decoding a Value will fail but handle nonetheless.
          case f: Failure => {
            result = Some(f)
            break
          }
        }
      }
    }

    // If any failure is encountered return that, otherwise build the map and create a result.
    result match {
      case Some(err) => err
      case None => Successful(DecodeResult(Row(data.result:_*), remaining))
    }
  }

  override def encode(row: Row): Attempt[BitVector] = {
    // Encode each column as it appears in the column spec.
    val attempts: List[Attempt[BitVector]] = columnsWithCodecs.map { (column) =>
      val codec = column._2
      val columnName = column._1
      row.columns.get(columnName) match {
        case Some(null) => value.encode(Null)
        case Some(data) => codec.encode(data)
        case None => value.encode(Unset)
      }
    }

    // Fold all attempts into one bit vector.  The first failure encountered
    // will be the one returned.
    attempts.fold[Attempt[BitVector]](Successful(BitVector.empty)) {
      (acc, e) => acc match {
        case Successful(accBits) => e match {
          case Successful(eBits) => Successful(accBits ++ eBits)
          case f: Failure => f
        }
        case f: Failure => f
      }
    }
  }

  private [this] lazy val sizes = {
    columnsWithCodecs.map(_._2.sizeBound).fold(SizeBound.exact(0)) { (acc: SizeBound, s: SizeBound) =>
      acc + s
    }
  }

  override def sizeBound: SizeBound = sizes
}