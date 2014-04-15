package uk.co.scassandra.priming

import akka.io.IO
import spray.can.Http
import spray.routing._
import spray.util.LoggingContext
import akka.event.Logging
import com.typesafe.scalalogging.slf4j.Logging
import spray.json.{JsValue, JsString, RootJsonFormat, DefaultJsonProtocol}
import spray.httpx.SprayJsonSupport
import spray.http.StatusCodes
import akka.actor.Actor
import org.scassandra.cqlmessages._
import scala.Some
import uk.co.scassandra.ErrorMessage


object JsonImplicits extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object ConsistencyJsonFormat extends RootJsonFormat[Consistency] {
    def write(c: Consistency) = JsString(c.string)

    def read(value: JsValue) = value match {
      case JsString(value) => Consistency.fromString(value)
      case _ => throw new IllegalArgumentException("Expected Consistency as JsString")
    }
  }

  implicit val impThen = jsonFormat3(Then)
  implicit val impWhen = jsonFormat4(When)
  implicit val impPrimeQueryResult = jsonFormat2(PrimeQueryResult)
  implicit val impConnection = jsonFormat1(Connection)
  implicit val impQuery = jsonFormat2(Query)
  implicit val impPrimeCriteria = jsonFormat2(PrimeCriteria)
  implicit val impConflictingPrimes = jsonFormat1(ConflictingPrimes)
}

trait PrimingServerRoute extends HttpService with Logging {

  import JsonImplicits._

  implicit val primedResults: PrimedResults

  val ColumnTypeMapping = Map[String, ColumnType](
    "int" -> CqlInt,
    "boolean" -> CqlBoolean,
    "ascii" -> CqlAscii,
    "bigint" -> CqlBigint,
    "counter" -> CqlCounter,
    "blob" -> CqlBlob,
    "decimal" -> CqlDecimal,
    "double" -> CqlDouble,
    "float" -> CqlFloat,
    "text" -> CqlText,
    "timestamp" -> CqlTimestamp,
    "uuid" -> CqlUUID,
    "inet" -> CqlInet,
    "varint" -> CqlVarint,
    "timeuuid" -> CqlTimeUUID
  )

  val route = {
    path("prime") {
      post {
        entity(as[PrimeQueryResult]) {
          primeRequest =>
            complete {
              // add the deserialized JSON request to the map of prime requests
              val resultsAsList = primeRequest.then.rows.getOrElse(List())
              val then = primeRequest.then
              val result = then.result.map(Result.fromString(_)).getOrElse(Success)
              logger.debug("Column types " + primeRequest.then.column_types)
              val columnTypes = primeRequest.then.column_types match {
                case Some(types) => types.map({
                  case (key: String, value) => (key, ColumnTypeMapping.getOrElse(value, CqlVarchar))
                })
                case _ => Map[String, ColumnType]()
              }

              //check that all the columns in thr rows have a type
              val columnNamesInAllRows = resultsAsList.flatMap(row => row.keys).distinct

              val columnTypesWithMissingDefaultedToVarchar = columnNamesInAllRows.map(columnName => columnTypes.get(columnName) match {
                case Some(columnType) => (columnName, columnType)
                case None => (columnName, CqlVarchar)
              }).toMap

              logger.debug("Incoming when {}", primeRequest.when)
              val primeConsistencies = primeRequest.when.consistency match {
                case Some(consistencyMap) => consistencyMap.map(Consistency.fromString(_))
                case None => Consistency.all
              }
              try {
                val keyspace = emptyStringIfNone(primeRequest.when.keyspace)

                val table = emptyStringIfNone(primeRequest.when.table)

                primedResults.add(PrimeCriteria(primeRequest.when.query, primeConsistencies),
                  resultsAsList, result, columnTypesWithMissingDefaultedToVarchar, keyspace, table)
                StatusCodes.OK
              }
              catch {
                case e: IllegalStateException =>
                  StatusCodes.BadRequest -> new ConflictingPrimes(existingPrimes = primedResults.getPrimeCriteriaByQuery(primeRequest.when.query))
              }
            }
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded priming")
            primedResults.clear()
            logger.debug("Return 200")
            StatusCodes.OK
          }
        }
    }
  } ~
    path("connection") {
      get {
        complete {
          ActivityLog.retrieveConnections()
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded connections")
            ActivityLog.clearConnections()
            StatusCodes.OK
          }
        }
    } ~
    path("query") {
      get {
        complete {
          logger.debug("Request for recorded queries")
          ActivityLog.retrieveQueries()
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded queries")
            ActivityLog.clearQueries()
            StatusCodes.OK
          }
        }
    }

  def emptyStringIfNone(option: Option[String]): String = option match {
    case Some(s) => s
    case None => ""
  }
}

class PrimingServer(port: Int, implicit val primedResults: PrimedResults) extends Actor with PrimingServerRoute with Logging {

  implicit def actorRefFactory = context.system

  logger.info(s"Opening port ${port} for priming")

  IO(Http) ! Http.Bind(self, "localhost", port)

  // some default spray initialisation
  val routingSettings = RoutingSettings default context

  val loggingContext = LoggingContext fromAdapter Logging(context.system, this)

  val exceptionHandler = ExceptionHandler default(routingSettings, loggingContext)

  def receive = runRoute(route)(exceptionHandler, RejectionHandler.Default, context, routingSettings, loggingContext)

  logger.info(s"Server bound to port $port")
}

case class ConflictingPrimes(existingPrimes: List[PrimeCriteria]) extends ErrorMessage("Conflicting Primes");
