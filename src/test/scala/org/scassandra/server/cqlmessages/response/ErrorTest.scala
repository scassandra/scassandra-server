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

import org.apache.cassandra.db.WriteType
import org.scalatest.{Matchers, FunSuite}
import org.scassandra.server.cqlmessages._
import org.scassandra.server.cqlmessages.VersionTwo
import org.scassandra.server.priming.{WriteRequestTimeoutResult, ReadRequestTimeoutResult}

class ErrorTest extends FunSuite with Matchers {

  import CqlProtocolHelper._

  implicit val protocolVersion = VersionTwo
  val defaultStream: Byte = 0x1

  test("Serialisation of a error response client protocol error") {
    val errorCode: Byte = 0xA
    val errorText = "Any old error message"
    val stream: Byte = 0x04
    val errorMessage = new response.Error(protocolVersion, errorCode, errorText, stream)
    val bytes: List[Byte] = errorMessage.serialize().toList

    bytes should equal(List[Byte](
      protocolVersion.serverCode, // protocol version
      0x00, // flags
      stream, // stream
      OpCodes.Error,
      0x0, 0x0, 0x0, (errorText.length + 6).toByte, // 4 byte integer - length (number of bytes)
      0x0, 0x0, 0x0, errorCode,
      0x00, errorText.length.toByte) ::: // length of the errorText
      errorText.getBytes.toList
    )
  }

  test("QueryBeforeReadyMessage is Error message with code 0xA") {
    val stream: Byte = 0x03
    val errorQueryBeforeReadyMessage = QueryBeforeReadyMessage(stream)

    errorQueryBeforeReadyMessage.header.opCode should equal(OpCodes.Error)
    errorQueryBeforeReadyMessage.errorCode should equal(0xA)
    errorQueryBeforeReadyMessage.errorMessage should equal("Query sent before StartUp message")
    errorQueryBeforeReadyMessage.header.streamId should equal(stream)
  }

  test("Read request timeout has error code 0x1200 and message Read Request Timeout") {
    val required = 2
    val actual = 1
    val dataPresent = false
    val readRequestTimeoutResult = ReadRequestTimeoutResult(actual, required, dataPresent)
    val consistency = TWO
    val readTimeout = ReadRequestTimeout(defaultStream, consistency, readRequestTimeoutResult)

    readTimeout.errorCode should equal(ErrorCodes.ReadTimeout)
    readTimeout.errorMessage should equal("Read Request Timeout")
    readTimeout.receivedResponses should equal(actual)
    readTimeout.blockFor should equal(required)
    readTimeout.dataPresent should equal(0)
  }

  test("Serialization of Read Request Timeout - hard coded data for now") {
    val providedConsistency = TWO
    val readTimeoutBytes = ReadRequestTimeout(defaultStream, providedConsistency, ReadRequestTimeoutResult()).serialize().iterator

    val header = readTimeoutBytes.drop(4)
    val length = readTimeoutBytes.getInt
    val errorCode = readTimeoutBytes.getInt
    errorCode should equal(ErrorCodes.ReadTimeout)
    // error message - string
    val errorString = CqlProtocolHelper.readString(readTimeoutBytes)
    val consistency = readTimeoutBytes.getShort
    consistency should equal(providedConsistency.code)
    val receivedResponses = readTimeoutBytes.getInt
    receivedResponses should equal(0)
    val blockedFor = readTimeoutBytes.getInt
    blockedFor should equal(1)
    val dataPresent = readTimeoutBytes.getByte
    dataPresent should equal(0)

    length should equal(4 + 2 + errorString.length + 2 + 4 + 4 + 1)
  }

  test("Serialization of Write Request Timeout - hard coded data for now") {
    val stream: Byte = 0x4
    val providedConsistency = QUORUM
    val receivedResponsesExpected: Int = 2
    val requiredResponsesExpected: Int = 3
    val writeTypeExpected: WriteType = WriteType.CAS
    val writeTimeoutResult = WriteRequestTimeoutResult(receivedResponsesExpected, requiredResponsesExpected, writeTypeExpected)
    val writeTimeoutBytes = WriteRequestTimeout(stream, providedConsistency, writeTimeoutResult).serialize().iterator
    // drop the header
    writeTimeoutBytes.drop(4)
    // drop the length
    writeTimeoutBytes.drop(4)

    val errorCode = writeTimeoutBytes.getInt
    errorCode should equal(ErrorCodes.WriteTimeout)

    val errorString = CqlProtocolHelper.readString(writeTimeoutBytes)
    errorString should equal("Write Request Timeout")

    val consistency = writeTimeoutBytes.getShort
    consistency should equal(providedConsistency.code)

    val receivedResponses = writeTimeoutBytes.getInt
    receivedResponses should equal(receivedResponsesExpected)

    val blockedFor = writeTimeoutBytes.getInt
    blockedFor should equal(requiredResponsesExpected)

    val writeType = CqlProtocolHelper.readString(writeTimeoutBytes)
    writeType should equal(writeTypeExpected.toString)
  }

  test("Serialization of Unavailable Exception - hard coded data for now") {
    val stream: Byte = 0x1
    val providedConsistency = ALL
    val unavailableException = UnavailableException(stream, providedConsistency).serialize().iterator
    // header
    val header = unavailableException.drop(4)
    // length
    val length = unavailableException.getInt
    // error code - int
    val errorCode = unavailableException.getInt
    errorCode should equal(ErrorCodes.UnavailableException)
    // error message - string
    val errorString = CqlProtocolHelper.readString(unavailableException)
    // consistency - short 0x0001    ONE
    val consistency = unavailableException.getShort
    consistency should equal(providedConsistency.code)
    // required - hard coded to 1
    val requiredResponses = unavailableException.getInt
    requiredResponses should equal(1)
    // alive - hard coded to 0
    val alive = unavailableException.getInt
    alive should equal(0)

    length should equal(4 + 2 + errorString.length + 2 + 4 + 4)
  }

  test("Serialisation of unsupported version exception") {
    val stream: Byte = 0x03
    val unsupportedProtocolVersion = UnsupportedProtocolVersion(stream)

    unsupportedProtocolVersion.header.opCode should equal(OpCodes.Error)
    unsupportedProtocolVersion.errorCode should equal(ErrorCodes.ProtocolError)
    unsupportedProtocolVersion.errorMessage should equal("Invalid or unsupported protocol version")
    unsupportedProtocolVersion.header.streamId should equal(stream)
  }
}
