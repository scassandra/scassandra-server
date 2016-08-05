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

import scodec.Codec
import scodec.codecs._

sealed trait MessageDirection
case object Request extends MessageDirection
case object Response extends MessageDirection

object MessageDirection {
  implicit val codec: Codec[MessageDirection] = mappedEnum(bool,
    Request -> false,
    Response -> true
  )
}

sealed case class ProtocolFlags(
  direction: MessageDirection,
  version: ProtocolVersion
) {
  val headerCodec: Codec[FrameHeader] = provide(this).flatPrepend(FrameHeader.withProtocol).as[FrameHeader]
}

object ProtocolFlags {
  implicit val codec: Codec[ProtocolFlags] = {
    ("direction" | MessageDirection.codec) ::
    ("version"   | ProtocolVersion.codec)
  }.as[ProtocolFlags]
}

sealed case class HeaderFlags(
  warning: Boolean = false,
  customPayload: Boolean = false,
  tracing: Boolean = false,
  compression: Boolean = false
)

object EmptyHeaderFlags extends HeaderFlags()

object HeaderFlags {
  implicit val codec: Codec[HeaderFlags] = {
    ("reserved"      | ignore(4)) ::
    ("warning"       | bool)      ::
    ("customPayload" | bool)      ::
    ("tracing"       | bool)      ::
    ("compression"   | bool)
  }.dropUnits.as[HeaderFlags]
}

case class FrameHeader (
  version: ProtocolFlags,
  flags: HeaderFlags = EmptyHeaderFlags,
  stream: Int = 0,
  opcode: Int = ErrorMessage.opcode,
  length: Long = 0
)

object FrameHeader {

  private[codec] def withProtocol(protocol: ProtocolFlags) = {
    ("flags"   | HeaderFlags.codec)              ::
    ("stream"  | protocol.version.streamIdCodec) ::
    ("opcode"  | uint8)                          ::
    ("length"  | uint32)
  }

  implicit val codec: Codec[FrameHeader] = ProtocolFlags.codec.flatPrepend(withProtocol).as[FrameHeader]
}
