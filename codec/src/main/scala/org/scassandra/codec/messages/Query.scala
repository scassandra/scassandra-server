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

import org.scassandra.codec.Consistency._
import org.scassandra.codec.Notations.{consistency, queryValue, bytes => cbytes, int => cint, long => clong, short => cshort}
import org.scassandra.codec.{Consistency, ProtocolVersion, QueryValue}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._
import shapeless.{::, HNil}

import scala.{:: => ~~}

case class QueryParameters(
  consistency: Consistency = ONE,
  values: Option[List[QueryValue]] = None,
  skipMetadata: Boolean = false,
  pageSize: Option[Int] = None,
  pagingState: Option[ByteVector] = None,
  serialConsistency: Option[Consistency] = None,
  timestamp: Option[Long] = None
)

object DefaultQueryParameters extends QueryParameters()

object QueryParameters {
  private[this] lazy val v1FlagsCodec = provide(QueryFlags())

  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[QueryParameters] = {
    ("consistency"        | Consistency.codec)                                                                ::
    ("flags"              | withDefault(conditional(protocolVersion.version > 1, Codec[QueryFlags]), v1FlagsCodec)).consume { flags =>
    ("values"             | conditional(flags.values, listOfN(cshort, queryValue(flags.namesForValues))))     ::
    ("skipMetadata"       | provide[Boolean](flags.skipMetadata))                                             ::
    ("pageSize"           | conditional(flags.pageSize, cint))                                                ::
    ("pagingState"        | conditional(flags.withPagingState, cbytes))                                       ::
    ("serialConsistency " | conditional(flags.withSerialConsistency, consistency))                            ::
    ("timestamp"          | conditional(flags.withDefaultTimestamp, clong))
  } { data =>
      // This is admittedly contrived, but allows us to forgo making QueryFlags part of QueryParameters as
      // they should be derivable by the parameters themselves.
      val values :: skipMetadata :: pageSize :: pagingState :: serialConsistency :: timestamp :: HNil = data
      val valuesPresent = values.isDefined && values.getOrElse(Nil).nonEmpty
      val namesForValues = values match {
        case Some(h ~~ tail) => h.name.isDefined
        case _ => false
      }
      QueryFlags(namesForValues, timestamp.isDefined, serialConsistency.isDefined, pagingState.isDefined,
        pageSize.isDefined, skipMetadata, valuesPresent)
    }
  }.as[QueryParameters]
}

private[this] case class QueryFlags(
  namesForValues: Boolean = false,
  withDefaultTimestamp: Boolean = false,
  withSerialConsistency: Boolean = false,
  withPagingState: Boolean = false,
  pageSize: Boolean = false,
  skipMetadata: Boolean = false,
  values: Boolean = false
)

private[this] object QueryFlags {
  implicit val codec: Codec[QueryFlags] = {
    ("reserved"          | ignore(1)) ::
    ("namesForValues"    | bool)      ::
    ("defaultTimestamp"  | bool)      ::
    ("serialConsistency" | bool)      ::
    ("pagingState"       | bool)      ::
    ("pageSize"          | bool)      ::
    ("skipMetadata"      | bool)      ::
    ("values"            | bool)
  }.as[QueryFlags]
}