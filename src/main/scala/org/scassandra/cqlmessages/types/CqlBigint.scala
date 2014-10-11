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
package org.scassandra.cqlmessages.types

import akka.util.ByteIterator
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlBigint extends ColumnType[Long](0x0002, "bigint") {
   override def readValue(byteIterator: ByteIterator) = {
     CqlProtocolHelper.readBigIntValue(byteIterator)
   }

   override def writeValue(value: Any) = {
     CqlProtocolHelper.serializeBigIntValue(value.toString.toLong)
   }
 }
