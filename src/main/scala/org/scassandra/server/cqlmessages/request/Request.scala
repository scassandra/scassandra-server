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
package org.scassandra.server.cqlmessages.request

import akka.util.ByteString
import org.scassandra.server.cqlmessages._

// Current has a fixed stream ID as only sent on startup
case object StartupHeader extends Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Startup, streamId = 0)

case class QueryHeader(stream: Byte) extends Header(ProtocolVersion.ClientProtocolVersionTwo, opCode = OpCodes.Query, streamId = stream)

abstract class Request(header: Header) extends CqlMessage(header)

object StartupRequest extends Request(StartupHeader) {

  val options = Map("CQL_VERSION" -> "3.0.0")

  override def serialize() = {
    val header = StartupHeader.serialize()

    val body = CqlProtocolHelper.serializeShort(options.size.toShort) ++
      CqlProtocolHelper.serializeString(options.head._1) ++
      CqlProtocolHelper.serializeString(options.head._2)

    ByteString((header ++ CqlProtocolHelper.serializeInt(body.size) ++ body))
  }
}

case class QueryRequest(stream: Byte, query: String, val consistency : Short = 0x0001, val flags : Byte = 0x00) extends Request(QueryHeader(stream)) {
  override def serialize() = {
    val body =  CqlProtocolHelper.serializeLongString(query) ++
      CqlProtocolHelper.serializeShort(consistency) ++
      CqlProtocolHelper.serializeByte(flags)

    ByteString(header.serialize() ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}

case class PrepareRequest(protocolVersion: Byte, stream: Byte, query: String) extends Request(new Header(protocolVersion, OpCodes.Prepare, stream)) {
  def serialize(): ByteString = {
    val headerBytes: Array[Byte] = header.serialize()
    val body: Array[Byte] = CqlProtocolHelper.serializeLongString(query)
    ByteString(headerBytes ++ CqlProtocolHelper.serializeInt(body.size) ++ body)
  }
}




