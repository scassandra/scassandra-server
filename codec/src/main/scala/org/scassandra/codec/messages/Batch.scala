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
package org.scassandra.codec.messages

import org.scassandra.codec.Notations.{bytes, longString, value, short => cshort}
import org.scassandra.codec.Value
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._

object BatchType extends Enumeration {
  type BatchType = Value
  val LOGGED, UNLOGGED, COUNTER = Value

  implicit val codec: Codec[BatchType] = enumerated(uint8, BatchType)
}

object BatchQueryKind extends Enumeration {
  type BatchQueryKind = Value
  val Simple, Prepared = Value

  implicit val codec: Codec[BatchQueryKind] = enumerated(uint8, BatchQueryKind)
}

case class BatchFlags(
  namesForValues: Boolean = false,
  withDefaultTimestamp: Boolean = false,
  withSerialConsistency: Boolean = false
)

object BatchFlags {
  implicit val codec: Codec[BatchFlags] = {
    ("reserved"          | ignore(5)) ::
    ("namesForValues"    | bool)      :: // Note that namesForValues is not currently used, see CASSANDRA-10246
    ("defaultTimestamp"  | bool)      ::
    ("serialConsistency" | bool)
  }.as[BatchFlags]
}

sealed trait BatchQuery {
  val values: List[Value]
}

object BatchQuery {
  implicit val codec: Codec[BatchQuery] = discriminated[BatchQuery].by(BatchQueryKind.codec)
    .typecase(BatchQueryKind.Simple, Codec[SimpleBatchQuery])
    .typecase(BatchQueryKind.Prepared, Codec[PreparedBatchQuery])
}

case class SimpleBatchQuery(query: String, values: List[Value] = Nil) extends BatchQuery

object SimpleBatchQuery {
  implicit val codec: Codec[SimpleBatchQuery] = (longString :: listOfN(cshort, value)).as[SimpleBatchQuery]
}

case class PreparedBatchQuery(id: ByteVector, values: List[Value] = Nil) extends BatchQuery

object PreparedBatchQuery {
  implicit val codec: Codec[PreparedBatchQuery] = (bytes :: listOfN(cshort, value)).as[PreparedBatchQuery]
}
