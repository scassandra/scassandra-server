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

sealed abstract class BatchQueryKind {
  val kind: Byte
  val string: String
}

case object QueryKind extends BatchQueryKind {
  val kind: Byte = 0
  val string: String = "query"
}

case object PreparedStatementKind extends BatchQueryKind {
  val kind: Byte = 1
  val string: String = "prepared_statement"
}

object BatchQueryKind {
  def fromString(string: String): BatchQueryKind = string match {
    case QueryKind.string => QueryKind
    case PreparedStatementKind.string => PreparedStatementKind
  }

  def fromCode(code: Byte): BatchQueryKind = code match {
    case QueryKind.kind => QueryKind
    case PreparedStatementKind.kind => PreparedStatementKind
  }
}