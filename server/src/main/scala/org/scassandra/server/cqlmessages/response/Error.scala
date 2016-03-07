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
package org.scassandra.server.cqlmessages.response

import akka.util.ByteString
import org.scassandra.server.cqlmessages._
import org.scassandra.server.priming.{UnavailableResult, ReadRequestTimeoutResult, WriteRequestTimeoutResult}

object ErrorCodes {
  val ServerError = 0x0000
  val ProtocolError = 0x000A
  val BadCredentials = 0x0100
  val UnavailableException = 0x1000
  val Overloaded = 0x1001
  val IsBootstrapping = 0x1002
  val TruncateError = 0x1003
  val WriteTimeout = 0x1100
  val ReadTimeout = 0x1200
  val SyntaxError = 0x2000
  val Unauthorized = 0x2100
  val Invalid = 0x2200
  val ConfigError = 0x2300
  val AlreadyExists = 0x2400
  val Unprepared = 0x2500
}

class Error(protocolVersion: ProtocolVersion, val errorCode : Int, val errorMessage : String, stream: Byte) extends Response(new Header(protocolVersion.serverCode, opCode = OpCodes.Error, streamId = stream)) {

  import org.scassandra.server.cqlmessages.CqlProtocolHelper._

  override def serialize() : ByteString = {
    val serialisedHeader: Array[Byte] = header.serialize()

    val bodyBuilder = ByteString.newBuilder
    bodyBuilder.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBuilder.putBytes(errorMessageBytes.toArray)
    val bodyBytes = bodyBuilder.result()
    
    combineHeaderAndLength(serialisedHeader, bodyBytes.toArray)
  }
}

case class ServerError(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ServerError, message, stream)

case class ProtocolError(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ProtocolError, message, stream)

case class UnsupportedProtocolVersion(stream: Byte)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ProtocolError, "Invalid or unsupported protocol version", stream)

case class QueryBeforeReadyMessage(stream: Byte = ResponseHeader.DefaultStreamId)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ProtocolError, "Query sent before StartUp message", stream)

case class BadCredentials(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.BadCredentials, message, stream)

case class UnavailableException(stream: Byte, consistency: Consistency, unavailableResult: UnavailableResult)
                               (implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.UnavailableException, "Unavailable Exception", stream) {

  import org.scassandra.server.cqlmessages.CqlProtocolHelper._

  val required : Int = unavailableResult.requiredResponses
  val alive : Int = unavailableResult.alive
  val consistencyToUse: Consistency = unavailableResult.consistencyLevel.getOrElse(consistency)


  override def serialize() : ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBs.putBytes(errorMessageBytes.toArray)
    bodyBs.putShort(consistencyToUse.code)
    bodyBs.putInt(required)
    bodyBs.putInt(alive)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class Overloaded(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.Overloaded, message, stream)

case class IsBootstrapping(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.IsBootstrapping, message, stream)

case class TruncateError(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.TruncateError, message, stream)

case class WriteRequestTimeout(stream: Byte, consistency: Consistency, writeRequestTimeoutResult: WriteRequestTimeoutResult)
                              (implicit protocolVersion: ProtocolVersion)
  extends Error(protocolVersion, ErrorCodes.WriteTimeout, "Write Request Timeout", stream) {

  import org.scassandra.server.cqlmessages.CqlProtocolHelper._

  val receivedResponses : Int = writeRequestTimeoutResult.receivedResponses
  val blockFor : Int = writeRequestTimeoutResult.requiredResponses
  val writeType : String = writeRequestTimeoutResult.writeType.toString
  val consistencyToUse: Consistency = writeRequestTimeoutResult.consistencyLevel.getOrElse(consistency)

  override def serialize() : ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBs.putBytes(errorMessageBytes.toArray)
    bodyBs.putShort(consistencyToUse.code)
    bodyBs.putInt(receivedResponses)
    bodyBs.putInt(blockFor)
    bodyBs.putBytes(CqlProtocolHelper.serializeString(writeType).toArray)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class ReadRequestTimeout(stream : Byte, consistency: Consistency, readRequestTimeoutResult: ReadRequestTimeoutResult)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ReadTimeout, "Read Request Timeout", stream) {

  val receivedResponses: Int = readRequestTimeoutResult.receivedResponses
  val blockFor: Int = readRequestTimeoutResult.requiredResponses
  val dataPresent: Byte = if (readRequestTimeoutResult.dataPresent) 1 else 0
  val consistencyToUse: Consistency = readRequestTimeoutResult.consistencyLevel.getOrElse(consistency)

  import org.scassandra.server.cqlmessages.CqlProtocolHelper._

  override def serialize() : ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBs.putBytes(errorMessageBytes.toArray)
    bodyBs.putShort(consistencyToUse.code)
    bodyBs.putInt(receivedResponses)
    bodyBs.putInt(blockFor)
    bodyBs.putByte(dataPresent)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class SyntaxError(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.SyntaxError, message, stream)

case class Unauthorized(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.Unauthorized, message, stream)

case class Invalid(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.Invalid, message, stream)

case class ConfigError(stream: Byte, message: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ConfigError, message, stream)

case class AlreadyExists(stream: Byte, message: String, keyspace: String, table: String)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.AlreadyExists, message, stream) {
  import CqlProtocolHelper._

  override def serialize(): ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    bodyBs.putBytes(serializeString(message))
    bodyBs.putBytes(serializeString(keyspace))
    bodyBs.putBytes(serializeString(table))

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class Unprepared(stream: Byte, message: String, id: Array[Byte])(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.Unprepared, message, stream) {
  import CqlProtocolHelper._

  override def serialize(): ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    bodyBs.putBytes(serializeString(message))
    bodyBs.putBytes(serializeShortBytes(id))

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}
