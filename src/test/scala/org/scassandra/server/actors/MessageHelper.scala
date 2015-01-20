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
package org.scassandra.server

import akka.util.ByteString
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.response.ResponseHeader

object MessageHelper {
  def dropHeaderAndLength(bytes: Array[Byte]) : Array[Byte] = {
    bytes drop 8 // drops the header and length
  }

  import CqlProtocolHelper._

  def createQueryMessage(queryString : String, stream : Byte = ResponseHeader.DefaultStreamId, consistency: Consistency = ONE, protocolVersion : Byte = ProtocolVersion.ClientProtocolVersionTwo) : List[Byte] = {
    val bodyLength = serializeInt(queryString.size + 4 + 2 + 1)
    val header = List[Byte](protocolVersion, 0x00, stream, OpCodes.Query) :::
      bodyLength

    val body : List[Byte] =
      serializeLongString(queryString) :::
        serializeShort(consistency.code) ::: // consistency
        List[Byte](0x00) ::: // query flags
        List[Byte]()

    header ::: body
  }

  def createStartupMessage(protocolVersion: ProtocolVersion = VersionTwo, stream: Byte = 0x0) : List[Byte] = {
    val messageBody = List[Byte](0x0, 0x1 , // number of start up options
    0x0, "CQL_VERSION".length.toByte)  :::
    "CQL_VERSION".getBytes.toList :::
      List[Byte](0x0, "3.0.0".length.toByte) :::
      "3.0.0".getBytes.toList

    val header: List[Byte] = protocolVersion match {
      case VersionThree => List[Byte](protocolVersion.clientCode, stream, 0x0, 0x0, OpCodes.Startup)
      case _ => List[Byte](protocolVersion.clientCode, stream, 0x0, OpCodes.Startup)
    }
    val bytes =
      header :::
      List[Byte](0x0, 0x0, 0x0, messageBody.length.toByte) :::
      messageBody

    bytes
  }

  def createRegisterMessage(protocolVersion : Byte = ProtocolVersion.ClientProtocolVersionTwo, stream : Byte = 0) : List[Byte] = {
    val header = List[Byte](
      protocolVersion,
      0x0,
      stream,
      OpCodes.Register
    )
    // TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE
    val registerBody = createRegisterMessageBody()

    header ::: serializeInt(registerBody.length) ::: registerBody
  }

  def createRegisterMessageBody(event : String = "TOPOLOGY_CHANGE") : List[Byte] = {
    val numberOfOptions = serializeShort(1)

    val singleOption = serializeString(event)

    numberOfOptions ::: singleOption
  }


  private def serializeLongString(string: String): List[Byte] = {
    serializeInt(string.length) :::
      serializeString(string)
  }
  private def serializeInt(int: Int): List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(int)
    frameBuilder.result().toList
  }
  private def serializeString(string: String): List[Byte] = {
    string.getBytes.toList
  }
  private def serializeShort(short : Short) : List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(short)
    frameBuilder.result().toList
  }

}
