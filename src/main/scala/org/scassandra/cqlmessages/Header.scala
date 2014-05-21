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
package org.scassandra.cqlmessages

import akka.util.ByteString

object Header {

  def Length = 4

  def apply(version : Byte, opCode : Byte, stream : Byte) : Header = {
    new Header(version, opCode, stream)
  }
}

class Header(val version : Int, val opCode : Byte, val streamId : Byte, val flags : Byte = 0x0) {

  def serialize() : Array[Byte] = {
    val bs = ByteString.newBuilder

    bs.putByte(versionAsByte())
    bs.putByte(flags.toByte)
    bs.putByte(streamId)
    bs.putByte(opCode.toByte)

    bs.result().toArray
  }

  def versionAsByte() : Byte = {
    (version & 0xFF).toByte
  }

}
