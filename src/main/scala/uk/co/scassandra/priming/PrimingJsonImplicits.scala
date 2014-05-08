package uk.co.scassandra.priming

import spray.json._
import spray.httpx.SprayJsonSupport
import uk.co.scassandra.cqlmessages.Consistency
import uk.co.scassandra.priming.query.{When, Then, PrimeQuerySingle, PrimeCriteria}
import uk.co.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedSingle}

object PrimingJsonImplicits extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object ConsistencyJsonFormat extends RootJsonFormat[Consistency] {
    def write(c: Consistency) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(consistency) => Consistency.fromString(consistency)
      case _ => throw new IllegalArgumentException("Expected Consistency as JsString")
    }
  }

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case s: String => JsString(s)
      case x: Seq[_] => seqFormat[Any].write(x)
      case m: Map[String, _] => mapFormat[String, Any].write(m)
      case b: Boolean if b == true => JsTrue
      case b: Boolean if b == false => JsFalse
      case set: Set[Any] => setFormat[Any].write(set)
      case x => serializationError("Do not understand object of type " + x.getClass.getName)
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
  implicit val impWhenPreparedSingle = jsonFormat1(WhenPreparedSingle)
  implicit val impThenPreparedSingle = jsonFormat1(ThenPreparedSingle)
  implicit val impPrimePreparedSingle = jsonFormat2(PrimePreparedSingle)
}
