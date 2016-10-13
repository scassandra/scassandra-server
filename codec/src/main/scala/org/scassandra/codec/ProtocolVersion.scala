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

import java.util.concurrent.ConcurrentHashMap

import org.scassandra.codec.Notations.{short => cshort, string => cstring}
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.{ColumnSpec, PreparedMetadata, QueryParameters, RowMetadata}
import scodec.Attempt.Successful
import scodec.Codec
import scodec.codecs._

import scala.collection.convert.decorateAsScala._
import scalaz.Memo

sealed trait ProtocolVersion {
  val version: Int
  lazy val headerLength: Long = 7 + (streamIdCodec.sizeBound.exact.get / 8)

  // cache message codecs as they are requested.  This prevents repeated creation of codecs.
  private[codec] lazy val memo = Memo.mutableMapMemo(new ConcurrentHashMap[Int, Codec[Message]]().asScala) {
    o: Int => Message.codecForVersion(this, o)
  }

  // codecs that have protocol version specific behavior.
  private[codec] val streamIdCodec: Codec[Int]
  private[codec] val collectionLengthCodec: Codec[Int]
  private[codec] val dataTypeCodec: Codec[DataType]
  private[codec] def messageCodec(opcode: Int): Codec[Message] = memo(opcode)
  private[codec] lazy val batchCodec: Codec[Batch] = Batch.codecForVersion(this)
  private[codec] lazy val executeCodec: Codec[Execute] = Execute.codecForVersion(this)
  private[codec] lazy val preparedCodec: Codec[Prepared] = Prepared.codecForVersion(this)
  private[codec] lazy val preparedMetadataCodec: Codec[PreparedMetadata] = PreparedMetadata.codecForVersion(this)
  private[codec] lazy val queryCodec: Codec[Query] = Query.codecForVersion(this)
  private[codec] lazy val queryParametersCodec: Codec[QueryParameters] = QueryParameters.codecForVersion(this)
  private[codec] lazy val resultCodec: Codec[Result] = Result.codecForVersion(this)
  private[codec] lazy val rowMetadataCodec: Codec[RowMetadata] = RowMetadata.codecForVersion(this)
  private[codec] lazy val rowsCodec: Codec[Rows] = Rows.codecForVersion(this)
  private[codec] lazy val columnSpecWithTableCodec: Codec[ColumnSpec] = ColumnSpec.codecForVersion(withTable = true)(this)
  private[codec] lazy val columnSpecWithoutTableCodec: Codec[ColumnSpec] = ColumnSpec.codecForVersion(withTable = false)(this)
}

sealed trait Int8StreamId {
  val streamIdCodec = int8
}

sealed trait Int16StreamId {
  val streamIdCodec = int16
}

sealed trait Uint16CollectionLength {
  val collectionLengthCodec = int16
}

sealed trait Uint32CollectionLength {
  val collectionLengthCodec = int32
}

case object ProtocolVersionV1 extends ProtocolVersion
  with Int8StreamId
  with Uint16CollectionLength {
  override val version = 1
  override val dataTypeCodec = ProtocolVersion.v1v2DataTypeCodec
}

case object ProtocolVersionV2 extends ProtocolVersion
  with Int8StreamId
  with Uint16CollectionLength {
  override val version = 2
  override val dataTypeCodec = ProtocolVersion.v1v2DataTypeCodec
}

case object ProtocolVersionV3 extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  override val version = 3
  override val dataTypeCodec = ProtocolVersion.v3DataTypeCodec()
}

case object ProtocolVersionV4 extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  override val version = 4
  override val dataTypeCodec = ProtocolVersion.v4DataTypeCodec()
}

case class UnsupportedProtocolVersion(version: Int) extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  // doesn't particularly matter for this case.
  override val dataTypeCodec = ProtocolVersion.v1v2DataTypeCodec
}

object ProtocolVersion {

  implicit val codec: Codec[ProtocolVersion] = uint(7).narrow({
    case 1 => Successful(ProtocolVersionV1)
    case 2 => Successful(ProtocolVersionV2)
    case 3 => Successful(ProtocolVersionV3)
    case 4 => Successful(ProtocolVersionV4)
    case x => Successful(UnsupportedProtocolVersion(x))
  }, _.version)

  val versions: List[ProtocolVersion] = ProtocolVersionV1 :: ProtocolVersionV2 :: ProtocolVersionV3 ::
    ProtocolVersionV4 :: Nil

  val latest: ProtocolVersion = ProtocolVersionV4

  private[this] def baseDesc = discriminated[DataType].by(cshort)

  // The v1/v2 codec minus the text type.
  // NOTE: That the odd behavior of passing in base discriminators is to facilitate lazily evaluated codecs
  // (tuples, lists, etc.) that need a forward reference to the codec to encode/decode elements.
  private[this] def baseDataTypeCodec(base: DiscriminatorCodec[DataType, Int]) = {
    lazy val codec: DiscriminatorCodec[DataType, Int] = base
      .typecase(0x00, cstring.as[DataType.Custom])
      .typecase(0x01, provide(DataType.Ascii))
      .typecase(0x02, provide(DataType.Bigint))
      .typecase(0x03, provide(DataType.Blob))
      .typecase(0x04, provide(DataType.Boolean))
      .typecase(0x05, provide(DataType.Counter))
      .typecase(0x06, provide(DataType.Decimal))
      .typecase(0x07, provide(DataType.Double))
      .typecase(0x08, provide(DataType.Float))
      .typecase(0x09, provide(DataType.Int))
      .typecase(0x0B, provide(DataType.Timestamp))
      .typecase(0x0C, provide(DataType.Uuid))
      .typecase(0x0D, provide(DataType.Varchar))
      .typecase(0x0E, provide(DataType.Varint))
      .typecase(0x0F, provide(DataType.Timeuuid))
      .typecase(0x10, provide(DataType.Inet))
      .typecase(0x20, lazily(codec.as[DataType.List]))
      .typecase(0x21, lazily((codec :: codec).as[DataType.Map]))
      .typecase(0x22, lazily(codec.as[DataType.Set]))
    codec
  }

  private[codec] def v1v2DataTypeCodec: DiscriminatorCodec[DataType, Int] =
    baseDataTypeCodec(baseDesc.typecase(0x0A, provide(DataType.Text)))

  private[codec] def v3DataTypeCodec(base: Option[DiscriminatorCodec[DataType, Int]] = None) = {
    lazy val codec: DiscriminatorCodec[DataType, Int] = baseDataTypeCodec(
      base.getOrElse(baseDesc)
        .typecase(0x0D, provide(DataType.Text)) // As Text type has been removed, alias it to varchars opcode.
        .typecase(0x31, lazily(cshort.consume(count => listOfN(provide(count), codec))(_.size).as[DataType.Tuple]))
    )
    codec
  }

  private[codec] def v4DataTypeCodec(base: Option[DiscriminatorCodec[DataType, Int]] = None) = v3DataTypeCodec(
    Some(base.getOrElse(baseDesc)
      .typecase(0x11, provide(DataType.Date))
      .typecase(0x12, provide(DataType.Time))
      .typecase(0x13, provide(DataType.Smallint))
      .typecase(0x14, provide(DataType.Tinyint)))
  )

}
