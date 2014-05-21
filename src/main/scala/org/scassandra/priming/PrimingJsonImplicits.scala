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
package org.scassandra.priming

import spray.json._
import spray.httpx.SprayJsonSupport
import org.scassandra.cqlmessages.ColumnType
import org.scassandra.cqlmessages.Consistency
import org.scassandra.priming.query.PrimeCriteria
import org.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedSingle}
import org.scassandra.priming.query.PrimeQuerySingle
import org.scassandra.priming.query.When
import org.scassandra.priming.query.Then

object PrimingJsonImplicits extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object ConsistencyJsonFormat extends RootJsonFormat[Consistency] {
    def write(c: Consistency) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(consistency) => Consistency.fromString(consistency)
      case _ => throw new IllegalArgumentException("Expected Consistency as JsString")
    }
  }

  implicit object ColumnTypeJsonFormat extends RootJsonFormat[ColumnType[_]] {
    def write(c: ColumnType[_]) = JsString(c.stringRep)

    def read(value: JsValue) = value match {
      case JsString(string) => ColumnType.fromString(string).get
      case _ => throw new IllegalArgumentException("Expected ColumnType as JsString")
    }
  }

  implicit object ResultJsonFormat extends RootJsonFormat[Result] {
    def write(result: Result) = JsString(result.string)

    def read(value: JsValue) = value match {
      case JsString(string) => Result.fromString(string)
      case _ => throw new IllegalArgumentException("Expected Result as JsString")
    }
  }

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case s: String => JsString(s)
      case seq: Seq[_] => seqFormat[Any].write(seq)
      case m: Map[String, _] => mapFormat[String, Any].write(m)
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
      case set: Set[Any] => setFormat[Any].write(set)
      case other => serializationError("Do not understand object of type " + other.getClass.getName)
    }
    def read(value: JsValue) = value match {
      case JsNumber(n) => n.longValue()
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => deserializationError("Do not understand how to deserialize " + x)
    }
  }

  implicit val impThen = jsonFormat3(Then)
  implicit val impWhen = jsonFormat4(When)
  implicit val impPrimeQueryResult = jsonFormat2(PrimeQuerySingle)
  implicit val impConnection = jsonFormat1(Connection)
  implicit val impQuery = jsonFormat2(Query)
  implicit val impPrimeCriteria = jsonFormat2(PrimeCriteria)
  implicit val impConflictingPrimes = jsonFormat1(ConflictingPrimes)
  implicit val impTypeMismatch = jsonFormat3(TypeMismatch)
  implicit val impTypeMismatches = jsonFormat1(TypeMismatches)
  implicit val impWhenPreparedSingle = jsonFormat2(WhenPreparedSingle)
  implicit val impThenPreparedSingle = jsonFormat4(ThenPreparedSingle)
  implicit val impPrimePreparedSingle = jsonFormat2(PrimePreparedSingle)
  implicit val impPreparedStatementExecution = jsonFormat3(PreparedStatementExecution)
}
