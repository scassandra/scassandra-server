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

import scodec.Attempt.Successful
import scodec.Codec
import scodec.codecs._

import scala.util.{Failure => TFailure}

sealed trait ProtocolVersion {
  val version: Int
  val streamIdCodec: Codec[Int]
  val collectionLengthCodec: Codec[Int]
  lazy val headerLength: Long = 7 + (streamIdCodec.sizeBound.exact.get / 8)
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
}

case object ProtocolVersionV2 extends ProtocolVersion
  with Int8StreamId
  with Uint16CollectionLength {
  override val version = 2
}

case object ProtocolVersionV3 extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  override val version = 3
}

case object ProtocolVersionV4 extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength {
  override val version = 4
}

case class UnsupportedProtocolVersion(version: Int) extends ProtocolVersion
  with Int16StreamId
  with Uint32CollectionLength

object ProtocolVersion extends Enumeration(1) {

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
}
