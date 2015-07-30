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
package org.scassandra.server

import java.math.BigInteger
import java.net.InetAddress
import java.util.UUID

import org.scassandra.server.cqlmessages.{BatchType, Consistency}
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.{PrimePreparedSingle, ThenPreparedSingle, WhenPreparedSingle}
import org.scassandra.server.priming.query.{PrimeCriteria, PrimeQuerySingle, Then, When}
import org.scassandra.server.priming.routes.Version
import spray.httpx.SprayJsonSupport
import spray.json._

import scala.collection.Set

//todo extend the prod one and override the differences
object PrimingJsonImplicitsForTest extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object ConsistencyJsonFormat extends RootJsonFormat[Consistency] {
    def write(c: Consistency) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(consistency) => Consistency.fromString(consistency)
      case _ => throw new IllegalArgumentException("Expected Consistency as JsString")
    }
  }

  implicit object BatchTypeJsonFormat extends RootJsonFormat[BatchType] {
    def write(c: BatchType) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(batchType) => BatchType.fromString(batchType)
      case _ => throw new IllegalArgumentException("Expected BatchType as JsString")
    }
  }

  implicit object ColumnTypeJsonFormat extends RootJsonFormat[ColumnType[_]] {
    def write(c: ColumnType[_]) = JsString(c.stringRep)

    def read(value: JsValue) = value match {
      case JsString(string) => ColumnType.fromString(string) match {
        case Some(columnType) => columnType
        case None => throw new IllegalArgumentException("Not a valid column type " + value)
      }
      case _ => throw new IllegalArgumentException("Expected ColumnType as JsString")
    }
  }

  implicit object ResultJsonFormat extends RootJsonFormat[ResultJsonRepresentation] {
    def write(result: ResultJsonRepresentation) = JsString(result.string)

    def read(value: JsValue) = value match {
      case JsString(string) => ResultJsonRepresentation.fromString(string)
      case _ => throw new IllegalArgumentException("Expected Result as JsString")
    }
  }

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case bd: BigDecimal => JsNumber(bd)
      case s: String => JsString(s)
      case seq: Seq[_] => seqFormat[Any].write(seq)
      case m: Map[_, _] => {
        val keysAsString: Map[String, Any] = m.map({ case (k, v) => (k.toString, v)})
        mapFormat[String, Any].write(keysAsString)
      }
      case set: Set[_] => setFormat[Any].write(set.map(s => s))
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
      case double: Double => JsNumber(double)
      case float: Float => JsNumber(float)
      case uuid: UUID => JsString(uuid.toString)
      case bigInt: BigInt => JsNumber(bigInt)
      case bigInt: BigInteger => JsNumber(bigInt)
      case bigD: java.math.BigDecimal => JsNumber(bigD)
      case inet: InetAddress => JsString(inet.getHostAddress)
      case bytes: Array[Byte] => JsString("0x" + bytes2hex(bytes))
      case None => JsNull
      case Some(s) => AnyJsonFormat.write(s)
      case other => serializationError("Do not understand object of type " + other.getClass.getName)
    }
    def read(value: JsValue) = value match {
      case jsNumber : JsNumber => jsNumber.value
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => deserializationError("Do not understand how to deserialize " + x)
    }
    def bytes2hex(bytes: Array[Byte]): String = {
       bytes.map("%02x".format(_)).mkString
    }
  }

  implicit val impThen = jsonFormat6(Then)
  implicit val impWhen = jsonFormat5(When)
  implicit val impPrimeQueryResult = jsonFormat(PrimeQuerySingle, "when", "then")
  implicit val impConnection = jsonFormat1(Connection)
  implicit val impQuery = jsonFormat4(Query)
  implicit val impPrimeCriteria = jsonFormat3(PrimeCriteria)
  implicit val impConflictingPrimes = jsonFormat1(ConflictingPrimes)
  implicit val impTypeMismatch = jsonFormat3(TypeMismatch)
  implicit val impTypeMismatches = jsonFormat1(TypeMismatches)
  implicit val impWhenPreparedSingle = jsonFormat3(WhenPreparedSingle)
  implicit val impThenPreparedSingle = jsonFormat6(ThenPreparedSingle)
  implicit val impPrimePreparedSingle = jsonFormat(PrimePreparedSingle, "when", "then")
  implicit val impPreparedStatementExecution = jsonFormat4(PreparedStatementExecution)
  implicit val impBatchQuery = jsonFormat1(BatchQuery)
  implicit val impBatchExecution = jsonFormat3(BatchExecution)

  implicit val impVersion = jsonFormat1(Version)
}
