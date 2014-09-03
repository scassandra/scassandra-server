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

// only supports strings for now.
//todo change this to a types class
case class CqlSet(setType : ColumnType[_]) extends ColumnType[Option[Set[_]]](0x0022, s"set<${setType.stringRep}>") {
   override def readValue(byteIterator: ByteIterator): Option[Set[String]] = {
     CqlProtocolHelper.readVarcharSetValue(byteIterator)
   }

   def writeValue(value: Any) = {
     if (value.isInstanceOf[Set[_]] || value.isInstanceOf[Seq[_]]) {
       CqlProtocolHelper.serializeSet(value.asInstanceOf[Iterable[setType.type]], setType)
     } else {
       throw new IllegalArgumentException(s"Can't serialise ${value} as Set of ${setType}")
     }
   }
 }
