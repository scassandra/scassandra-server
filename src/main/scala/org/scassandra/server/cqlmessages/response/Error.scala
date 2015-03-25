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
import org.scassandra.server.priming.ReadRequestTimeoutResult

object ErrorCodes {
  val ProtocolError = 0x000A
  val ReadTimeout = 0x1200
  val UnavailableException = 0x1000
  val WriteTimeout = 0x1100
}

class Error(protocolVersion: ProtocolVersion, val errorCode : Int, val errorMessage : String, stream: Byte) extends Response(new Header(protocolVersion.serverCode, opCode = OpCodes.Error, streamId = stream)) {

  import CqlProtocolHelper._

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

case class UnsupportedProtocolVersion(stream: Byte)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ProtocolError, "Invalid or unsupported protocol version", stream)

case class QueryBeforeReadyMessage(stream : Byte = ResponseHeader.DefaultStreamId)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ProtocolError, "Query sent before StartUp message", stream)

case class ReadRequestTimeout(stream : Byte, consistency: Consistency, readRequestTimeoutResult: ReadRequestTimeoutResult)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.ReadTimeout, "Read Request Timeout", stream) {

  val receivedResponses : Int = readRequestTimeoutResult.receivedResponses
  val blockFor : Int = readRequestTimeoutResult.requiredResponses
  val dataPresent : Byte = if (readRequestTimeoutResult.dataPresent) 1 else 0

  import CqlProtocolHelper._

  override def serialize() : ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBs.putBytes(errorMessageBytes.toArray)
    bodyBs.putShort(consistency.code)
    bodyBs.putInt(receivedResponses)
    bodyBs.putInt(blockFor)
    bodyBs.putByte(dataPresent)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class UnavailableException(stream: Byte, consistency: Consistency)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.UnavailableException, "Unavailable Exception", stream) {

  import CqlProtocolHelper._

  val required : Int = 1
  val alive : Int = 0

  override def serialize() : ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBs.putBytes(errorMessageBytes.toArray)
    bodyBs.putShort(consistency.code)
    bodyBs.putInt(required)
    bodyBs.putInt(alive)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}

case class WriteRequestTimeout(stream: Byte, consistency: Consistency)(implicit protocolVersion: ProtocolVersion) extends Error(protocolVersion, ErrorCodes.WriteTimeout, "Write Request Timeout", stream) {

  import CqlProtocolHelper._

  val receivedResponses : Int = 0
  val blockFor : Int = 1
  val writeType : String = WriteTypes.Simple

  override def serialize() : ByteString = {
    val bodyBs = ByteString.newBuilder
    bodyBs.putInt(errorCode)
    val errorMessageBytes = CqlProtocolHelper.serializeString(errorMessage)
    bodyBs.putBytes(errorMessageBytes.toArray)
    bodyBs.putShort(consistency.code)
    bodyBs.putInt(receivedResponses)
    bodyBs.putInt(blockFor)
    bodyBs.putBytes(CqlProtocolHelper.serializeString(writeType).toArray)

    combineHeaderAndLength(header.serialize(), bodyBs.result().toArray)
  }
}