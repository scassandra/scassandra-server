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

sealed trait ProtocolVersion {
  val version: Int
  val streamIdCodec: Codec[Int]
  val collectionLengthCodec: Codec[Long]
}

sealed trait Int8StreamId extends ProtocolVersion {
  override val streamIdCodec = int8
}

sealed trait Int16StreamId extends ProtocolVersion {
  override val streamIdCodec = int16
}

sealed trait Uint16CollectionLength extends ProtocolVersion {
  override val collectionLengthCodec = ulong(16)
}

sealed trait Uint32CollectionLength extends ProtocolVersion {
  override val collectionLengthCodec = uint32
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

object ProtocolVersion extends Enumeration(1) {
  implicit val codec: Codec[ProtocolVersion] = mappedEnum(uint(7),
    ProtocolVersionV1 -> 1,
    ProtocolVersionV2 -> 2,
    ProtocolVersionV3 -> 3,
    ProtocolVersionV4 -> 4
  )
}

sealed case class ProtocolFlags(
  direction: MessageDirection,
  version: ProtocolVersion
)

object ProtocolFlags {
  implicit val codec: Codec[ProtocolFlags] = {
    ("direction" | MessageDirection.codec) ::
    ("version"   | ProtocolVersion.codec)
  }.as[ProtocolFlags]
}

sealed case class HeaderFlags(
  warning: Boolean,
  customPayload: Boolean,
  tracing: Boolean,
  compression: Boolean
)

object HeaderFlags {
  implicit val codec: Codec[HeaderFlags] = {
    ("reserved"      | ignore(4)) ::
    ("warning"       | bool)      ::
    ("customPayload" | bool)      ::
    ("tracing"       | bool)      ::
    ("compression"   | bool)
  }.dropUnits.as[HeaderFlags]
}

object Opcode extends Enumeration(0) {
  type Opcode = Value
  val Error,
      Startup,
      Ready,
      Authenticate,
      Reserved,
      Options,
      Supported,
      Query,
      Result,
      Prepare,
      Execute,
      Register,
      Event,
      Batch,
      AuthChallenge,
      AuthResponse,
      AuthSuccess = Value

  implicit val codec: Codec[Opcode.Value] = enumerated(uint8, Opcode)
}

case class FrameHeader (
  version: ProtocolFlags,
  flags: HeaderFlags,
  stream: Int,
  opcode: Opcode.Value,
  length: Long
)

object FrameHeader {
  implicit val codec: Codec[FrameHeader] = {
    ("version" | ProtocolFlags.codec )            >>:~ { protocol =>
    ("flags"   | HeaderFlags.codec)               ::
    ("stream"  | protocol.version.streamIdCodec)  ::
    ("opcode"  | Opcode.codec)                    ::
    ("length"  | uint32)
  }}.as[FrameHeader]
}