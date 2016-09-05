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
package org.scassandra.server.cqlmessages

sealed abstract class BatchType {
  val code: Byte
  val string: String
}

object LOGGED extends BatchType {
  val code: Byte = 0
  val string: String = "LOGGED"
}
object UNLOGGED extends BatchType {
  val code: Byte = 1
  val string: String = "UNLOGGED"
}
object COUNTER extends BatchType {
  val code: Byte = 2
  val string: String = "COUNTER"
}

object BatchType {
  def fromString(batchType: String): BatchType = batchType match {
    case LOGGED.string => LOGGED
    case UNLOGGED.string => UNLOGGED
    case COUNTER.string => COUNTER
  }

  def fromCode(code: Byte): BatchType = {
    code match {
      case LOGGED.code => LOGGED
      case UNLOGGED.code => UNLOGGED
      case COUNTER.code => COUNTER
    }
  }
}


