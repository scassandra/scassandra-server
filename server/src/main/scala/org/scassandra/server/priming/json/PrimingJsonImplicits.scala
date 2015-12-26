package org.scassandra.server.priming.json

import java.math.BigInteger
import java.net.InetAddress
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.actors._
import org.scassandra.server.cqlmessages.types.ColumnType
import org.scassandra.server.cqlmessages.{BatchType, BatchQueryKind, Consistency}
import org.scassandra.server.priming._
import org.scassandra.server.priming.batch.{BatchQueryPrime, BatchWhen, BatchPrimeSingle}
import org.scassandra.server.priming.prepared._
import org.scassandra.server.priming.query.{PrimeCriteria, PrimeQuerySingle, When, Then}
import org.scassandra.server.priming.routes.Version
import spray.httpx.SprayJsonSupport
import spray.json._

import scala.collection.Set

object PrimingJsonImplicits extends DefaultJsonProtocol with SprayJsonSupport with LazyLogging {

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case bd: BigDecimal => JsString(bd.bigDecimal.toPlainString)
      case s: String => JsString(s)
      case seq: Seq[_] => seqFormat[Any].write(seq)
      case m: Map[_, _] =>
        val keysAsString: Map[String, Any] = m.map({ case (k, v) => (k.toString, v)})
        mapFormat[String, Any].write(keysAsString)
      case set: Set[_] => setFormat[Any].write(set.map(s => s))
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse

      // sending as strings to not lose precision
      case double: Double => JsString(double.toString)
      case float: Float => JsString(float.toString)
      case uuid: UUID => JsString(uuid.toString)
      case bigInt: BigInt => JsNumber(bigInt)
      case bigInt: BigInteger => JsNumber(bigInt)
      case bigD: java.math.BigDecimal => JsString(bigD.toPlainString)
      case inet: InetAddress => JsString(inet.getHostAddress)
      case bytes: Array[Byte] => JsString("0x" + bytes2hex(bytes))
      case None => JsNull
      case Some(s) => this.write(s)
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

  implicit object ConsistencyJsonFormat extends RootJsonFormat[Consistency] {
    def write(c: Consistency) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(consistency) => Consistency.fromString(consistency)
      case _ => throw new IllegalArgumentException("Expected Consistency as JsString")
    }
  }

  implicit object BatchQueryKindJsonFormat extends RootJsonFormat[BatchQueryKind] {
    def write(c: BatchQueryKind) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(v) => BatchQueryKind.fromString(v)
      case _ => throw new IllegalArgumentException("Expected BatchQueryKind as JsString")
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
        case None =>
          logger.warn(s"Received invalid column type $string")
          throw new IllegalArgumentException("Not a valid column type " + string)
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


  implicit val impThen = jsonFormat6(Then)
  implicit val impWhen = jsonFormat5(When)
  implicit val impPrimeQueryResult = jsonFormat(PrimeQuerySingle, "when", "then")
  implicit val impConnection = jsonFormat1(Connection)
  implicit val impQuery = jsonFormat4(Query)
  implicit val impPrimeCriteria = jsonFormat3(PrimeCriteria)
  implicit val impConflictingPrimes = jsonFormat1(ConflictingPrimes)
  implicit val impTypeMismatch = jsonFormat3(TypeMismatch)
  implicit val impTypeMismatches = jsonFormat1(TypeMismatches)
  implicit val impWhenPreparedSingle = jsonFormat3(WhenPrepared)
  implicit val impThenPreparedSingle = jsonFormat6(ThenPreparedSingle)
  implicit val impPrimePreparedSingle = jsonFormat(PrimePreparedSingle, "when", "then")
  implicit val impPreparedStatementExecution = jsonFormat4(PreparedStatementExecution)
  implicit val impPreparedStatementPreparation = jsonFormat1(PreparedStatementPreparation)
  implicit val impVersion = jsonFormat1(Version)
  implicit val impBatchQuery = jsonFormat3(BatchQuery)
  implicit val impBatchExecution = jsonFormat3(BatchExecution)
  implicit val impBatchQueryPrime = jsonFormat2(BatchQueryPrime)
  implicit val impBatchWhen = jsonFormat3(BatchWhen)
  implicit val impBatchPrimeSingle = jsonFormat(BatchPrimeSingle, "when", "then")
  implicit val impClientConnection = jsonFormat2(ClientConnection)
  implicit val impClientConnections = jsonFormat1(ClientConnections)
  implicit val impClosedConnections = jsonFormat(ClosedConnections, "closed_connections", "operation")
  implicit val impAcceptNewConnectionsEnabled = jsonFormat1(AcceptNewConnectionsEnabled)
  implicit val impRejectNewConnectionsEnabled = jsonFormat1(RejectNewConnectionsEnabled)

  implicit val impVariableMatch = jsonFormat1(VariableMatch)
  implicit val impCriterna = jsonFormat1(Criteria)
  implicit val impAction = jsonFormat5(Action)
  implicit val impOutcoe = jsonFormat2(Outcome)
  implicit val impPrimePreparedMultiThen = jsonFormat2(ThenPreparedMulti)
  implicit val impPrimePreparedMulti = jsonFormat(PrimePreparedMulti, "when", "then")

}
