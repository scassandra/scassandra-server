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

import org.scassandra.codec.Notations.{int => cint, short => cshort, string => cstring}
import org.scassandra.codec.ProtocolVersion
import scodec.Codec
import scodec.codecs._

case class PreparedMetadata(
  partitionKeyIndices: Option[List[Int]] = Some(Nil),
  keyspace: Option[String] = None,
  table: Option[String] = None,
  columnSpec: List[ColumnSpec] = Nil
)

object NoPreparedMetadata extends PreparedMetadata()

object PreparedMetadata {

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    ("flags"               | cint).consume { (flags: Int) =>
    ("columnCount"         | cint).consume { (columnCount: Int) =>
    ("partitionKeyIndices" | conditional(protocolVersion.version >= 4, listOfN(cint, cshort))) ::
    ("keyspace"            | conditional(flags == 1, cstring))                                 ::
    ("table"               | conditional(flags == 1, cstring))                                 ::
    ("columnSpec"          | listOfN(provide(columnCount), ColumnSpec.codec(flags == 0)))
  }(_.tail.tail.tail.head.size) // columnCount = columnSpec.size, TODO: use _(3) instead.
  }(data => if (data.tail.head.isDefined) 1 else 0) // flags.globalTableSpec == keyspace is present.
  }.as[PreparedMetadata]

  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[PreparedMetadata] =
    protocolVersion.preparedMetadataCodec
}