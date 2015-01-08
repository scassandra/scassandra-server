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

import org.scassandra.server.cqlmessages.CqlProtocolHelper
import org.scassandra.server.cqlmessages.types.{CqlMap, CqlList, CqlSet, ColumnType}
import akka.util.ByteStringBuilder

object ResultHelper {

  import CqlProtocolHelper._

  def serialiseTypeInfomration(name: String, columnType: ColumnType[_], iterator: ByteStringBuilder) = {
    iterator.putBytes(CqlProtocolHelper.serializeString(name).toArray)
    iterator.putShort(columnType.code)
    columnType match {
      case CqlSet(setType) => iterator.putShort(setType.code)
      case CqlList(listType) => iterator.putShort(listType.code)
      case CqlMap(keyType, valueType) => {
        iterator.putShort(keyType.code)
        iterator.putShort(valueType.code)
      }
      case _ => //no-op
    }
  }
}
