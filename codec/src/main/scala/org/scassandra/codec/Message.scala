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

import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.Notations.{bytes => cbytes, int => cint, long => clong, short => cshort, string => cstring, _}
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.codec.messages._
import scodec.Attempt.{Failure, Successful}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._
import shapeless.{::, HNil}

import scala.util.{Try, Failure => TFailure}

sealed trait Message {
  val opcode: Int

  def toBytes(stream: Int, direction: MessageDirection)(implicit protocolVersion: ProtocolVersion): Try[ByteVector] = {
    Message.codec(opcode).encode(this).flatMap(bits => {
      val bytes = bits.bytes
      val header = FrameHeader(
        ProtocolFlags(direction, protocolVersion),
        EmptyHeaderFlags,
        stream,
        opcode,
        bits.bytes.size
      )
      Codec[FrameHeader].encode(header).map(_.bytes ++ bytes)
    }) match {
      case Successful(result) => Try(result)
      case Failure(x) => TFailure(new Exception(x.toString))
    }
  }
}

object Message {

  /**
    * Resolves the appropriate [[Message]] codec based on the opcode.
    * @param opcode Opcode of the message to resolve codec for.
    * @return The appropriate [[Codec]] based on the opcode.
    */
  def codec(opcode: Int)(implicit protocolVersion: ProtocolVersion) =
    // the 'message' codec is resolved from the opcode determined in the header.
    // coproduct gathers all the codecs under that type.
    // discriminatedBy chooses the codec based on the provided opcode.
    // TODO: Consider switching to implicit discriminators, which currently isn't too IDE friendly.
    discriminated[Message].by(provide(opcode))
      .typecase(ErrorMessage.opcode, Codec[ErrorMessage])
      .typecase(Startup.opcode, Codec[Startup])
      .typecase(Ready.opcode, Codec[Ready.type])
      // TODO: Authenticate
      .typecase(Options.opcode, Codec[Options.type])
      .typecase(Supported.opcode, Codec[Supported])
      .typecase(Query.opcode, Codec[Query])
      .typecase(Result.opcode, Codec[Result])
      .typecase(Prepare.opcode, Codec[Prepare])
      .typecase(Execute.opcode, Codec[Execute])
      .typecase(Register.opcode, Codec[Register])
      // TODO: Event
      .typecase(Batch.opcode, Codec[Batch])
      // TODO: AuthChallenge
      // TODO: AuthResponse
      // TODO: AuthSuccess
}

sealed abstract class ErrorMessage(message: String) extends Message {
  override val opcode = ErrorMessage.opcode
}

case class ServerError(message: String) extends ErrorMessage(message)
case class ProtocolError(message: String) extends ErrorMessage(message)
case class BadCredentials(message: String) extends ErrorMessage(message)
case class Unavailable(message: String = "Unavailable Exception", consistency: Consistency = Consistency.ONE, required: Int = 1, alive: Int = 0) extends ErrorMessage(message)
case class Overloaded(message: String) extends ErrorMessage(message)
case class IsBootstrapping(message: String) extends ErrorMessage(message)
case class TruncateError(message: String) extends ErrorMessage(message)
// TODO: WriteType to its own type
case class WriteTimeout(message: String = "Write Request Timeout", consistency: Consistency = Consistency.ONE, received: Int = 1, blockFor: Int = 1, writeType: String = "SIMPLE") extends ErrorMessage(message)
case class ReadTimeout(message: String = "Read Request Timeout", consistency: Consistency = Consistency.ONE, received: Int = 1, blockFor: Int = 1, dataPresent: Boolean = false) extends ErrorMessage(message)
case class ReadFailure(message: String = "Read Failure", consistency: Consistency, received: Int = 1, blockFor: Int = 1, numFailures: Int = 1, dataPresent: Boolean = false) extends ErrorMessage(message)
case class FunctionFailure(message: String = "Function Failure", keyspace: String, function: String, argTypes: List[String]) extends ErrorMessage(message)
case class WriteFailure(message: String = "Write Failure", consistency: Consistency, received: Int, blockFor: Int, numFailures: Int, writeType: String) extends ErrorMessage(message)
case class SyntaxError(message: String) extends ErrorMessage(message)
case class Unauthorized(message: String) extends ErrorMessage(message)
case class Invalid(message: String) extends ErrorMessage(message)
case class ConfigError(message: String) extends ErrorMessage(message)
case class AlreadyExists(message: String, keyspace: String, table: String) extends ErrorMessage(message)
case class Unprepared(message: String, id: ByteVector) extends ErrorMessage(message)

object ErrorMessage {
  val opcode = 0x00
  implicit val codec: Codec[ErrorMessage] = discriminated[ErrorMessage].by(cint)
    .typecase(0x0000, cstring.as[ServerError])
    .typecase(0x000A, cstring.as[ProtocolError])
    .typecase(0x0100, cstring.as[BadCredentials])
    .typecase(0x1000, (cstring :: Consistency.codec :: cint :: cint).as[Unavailable])
    .typecase(0x1001, cstring.as[Overloaded])
    .typecase(0x1002, cstring.as[IsBootstrapping])
    .typecase(0x1003, cstring.as[TruncateError])
    .typecase(0x1100, (cstring :: Consistency.codec :: cint :: cint :: cstring).as[WriteTimeout])
    .typecase(0x1200, (cstring :: Consistency.codec :: cint :: cint :: bool(8)).as[ReadTimeout])
    .typecase(0x1300, (cstring :: Consistency.codec :: cint :: cint :: cint :: bool(8)).as[ReadFailure])
    .typecase(0x1400, (cstring :: cstring :: cstring :: stringList).as[FunctionFailure])
    .typecase(0x1500, (cstring :: Consistency.codec :: cint :: cint :: cint :: cstring).as[WriteFailure])
    .typecase(0x2000, cstring.as[SyntaxError])
    .typecase(0x2100, cstring.as[Unauthorized])
    .typecase(0x2200, cstring.as[Invalid])
    .typecase(0x2300, cstring.as[ConfigError])
    .typecase(0x2400, (cstring :: cstring :: cstring).as[AlreadyExists])
    .typecase(0x2500, (cstring :: shortBytes).as[Unprepared])
}

case class Startup(options: Map[String, String] = Map()) extends Message {
  override val opcode = Startup.opcode
}

object Startup {
  val opcode = 0x01
  implicit val codec: Codec[Startup] = stringMap.as[Startup]
}

case object Ready extends Message {
  override val opcode = 0x2
  implicit val codec: Codec[Ready.type] = provide(Ready)
}

case object Options extends Message {
  override val opcode = 0x5
  implicit val codec: Codec[Options.type] = provide(Options)
}

case class Supported(options: Map[String, List[String]] = Map()) extends Message {
  override val opcode = Supported.opcode
}

object Supported {
  val opcode = 0x6
  implicit val codec: Codec[Supported] = stringMultimap.as[Supported]
}

case class Query(query: String, parameters: QueryParameters = DefaultQueryParameters) extends Message {
  override val opcode = Query.opcode
}

object Query {
  val opcode = 0x7
  implicit val discriminator: Discriminator[Message, Query, Int] = Discriminator(opcode)
  implicit val codec: Codec[Query] = {
    ("query"      | longString) ::
    ("parameters" | Codec[QueryParameters])
  }.as[Query]
}

sealed trait Result extends Message {
  override val opcode: Int = Result.opcode
}

object Result {
  val opcode: Int = 0x8
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Result] = discriminated[Result].by(cint)
    .typecase(0x1, Codec[VoidResult.type])
    .typecase(0x2, Codec[Rows])
    .typecase(0x3, Codec[SetKeyspace])
    .typecase(0x4, Codec[Prepared])
}

case object VoidResult extends Result {
  implicit val codec: Codec[VoidResult.type] = provide(VoidResult)
}

case class Rows(metadata: RowMetadata = NoRowMetadata, rows: List[Row] = Nil) extends Result
object NoRows extends Rows()

object Rows {
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Rows] = {
    Codec[RowMetadata].flatPrepend{ metadata =>
      listOfN(cint, Row.withColumnSpec(metadata.columnSpec.getOrElse(Nil))).hlist
  }}.as[Rows]
}

case class SetKeyspace(keyspace: String) extends Result

case object SetKeyspace {
  implicit val codec: Codec[SetKeyspace] = cstring.as[SetKeyspace]
}

case class Prepared(id: ByteVector, preparedMetadata: PreparedMetadata = NoPreparedMetadata,
                    resultMetadata: RowMetadata = NoRowMetadata) extends Result

case object Prepared {
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Prepared] = {
    ("id"               | shortBytes)               ::
    ("preparedMetadata" | Codec[PreparedMetadata])  ::
    ("resultMetadata"   | Codec[RowMetadata])
  }.as[Prepared]
}

//case class SchemaChange extends Result

case class Prepare(query: String) extends Message{
  override val opcode = Prepare.opcode
}

object Prepare {
  val opcode = 0x9
  implicit val codec: Codec[Prepare] = longString.as[Prepare]
}

case class Execute(id: ByteVector, parameters: QueryParameters = DefaultQueryParameters) extends Message {
  val opcode = Execute.opcode
}

object Execute {
  val opcode = 0xA

  implicit val codec: Codec[Execute] = {
    ("id"              | shortBytes) ::
    ("queryParameters" | Codec[QueryParameters])
  }.as[Execute]
}


case class Register(events: List[String] = Nil) extends Message {
  override val opcode: Int = Register.opcode
}

object Register {
  val opcode = 0xB
  implicit val codec: Codec[Register] = stringList.as[Register]
}

case class Batch(
  batchType: BatchType,
  queries: List[BatchQuery],
  consistency: Consistency = Consistency.ONE,
  serialConsistency: Option[Consistency] = None,
  timestamp: Option[Long] = None
) extends Message {
  override val opcode = Batch.opcode
}

object Batch {
  val opcode = 0xD

  // TODO: Refactor this, it's currently really ugly to expand/unexpand values driven by batch flags in a way that
  // doesn't require any changes to the Batch class.

  /**
    * Flattens an <code>Option[ Option[Consistency] :: Option[Long] ]</code> HList into
    * <code>Option[Consistency] :: Option[Long]</code>
    * @return
    */
  def flatten(in: Option[::[Option[Consistency], ::[Option[Long], HNil]]]): ::[Option[Consistency], ::[Option[Long], HNil]] = {
    val (scon: Option[Consistency], ts: Option[Long]) = in match {
      case Some(x :: y :: HNil) => (x, y)
      case None => (None, None)
    }
    scon :: ts :: HNil
  }

  /**
    * Unflattens an Option[Consistency] :: Option[Long] HList into Option[ Option[Consistency] :: Option[Long] ]
    * @param in
    * @return
    */
  def unflatten(in: ::[Option[Consistency], ::[Option[Long], HNil]]): Option[::[Option[Consistency], ::[Option[Long], HNil]]] = in match {
    case None :: None :: HNil => None
    case Some(x) :: Some(y) :: HNil => Some(Some(x) :: Some(y) :: HNil)
    case Some(x) :: None :: HNil => Some(Some(x) :: None :: HNil)
    case None :: Some(y) :: HNil => Some(None :: Some(y) :: HNil)
  }

  def handleBatchFlags(implicit protocolVersion: ProtocolVersion) = conditional(protocolVersion.version >= ProtocolVersionV3.version,
    ("flags"             | Codec[BatchFlags]).consume { flags =>
    ("serialConsistency" | conditional(flags.withSerialConsistency, consistency)) ::
    ("timestamp"         | conditional(flags.withDefaultTimestamp, clong))
    } { data => // derive flags from presence of serialConsistency and timestamp.
      val serialConsistency = data.head
      val timestamp = data.tail.head
      BatchFlags(
        withDefaultTimestamp = timestamp.isDefined,
        withSerialConsistency = serialConsistency.isDefined
      )
    }).xmap(flatten, unflatten)

  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Batch] = {
    ("batchType"         | Codec[BatchType]) ::
    ("queries"           | listOfN(cshort, Codec[BatchQuery])) ::
    ("consistency"       | Consistency.codec) ::
    ("remaining"         | handleBatchFlags)
  }.as[Batch]
}
