package uk.co.scassandra.priming

import akka.io.IO
import spray.can.Http
import spray.routing._
import spray.util.LoggingContext
import akka.event.Logging
import com.typesafe.scalalogging.slf4j.Logging
import spray.json.DefaultJsonProtocol
import spray.httpx.SprayJsonSupport
import spray.http.StatusCodes
import akka.actor.Actor
import com.batey.narinc.client.cqlmessages._
import uk.co.scassandra.priming.Connection
import uk.co.scassandra.priming.PrimeQueryResult
import scala.Some
import uk.co.scassandra.priming.Then
import uk.co.scassandra.priming.Query

object JsonImplicits extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val impThen = jsonFormat3(Then)
  implicit val impPrimeQueryResult = jsonFormat2(PrimeQueryResult)
  implicit val impConnection = jsonFormat1(Connection)
  implicit val impQuery = jsonFormat1(Query)
}

trait PrimingServerRoute extends HttpService with Logging {

  import JsonImplicits._

  implicit val primedResults: PrimedResults

  val ColumnTypeMapping = Map[String, ColumnType](
    "int" -> CqlInt,
    "boolean" -> CqlBoolean,
    "ascii" -> CqlAscii
  )

  val route = {
    path("prime") {
      post {
        entity(as[PrimeQueryResult]) {
          primeRequest =>
            complete {
              // add the deserialized JSON request to the map of prime requests
              val resultsAsList = primeRequest.then.rows match {
                case Some(listOfRows) => listOfRows//listOfRows.convertTo[List[Map[String, String]]]
                case _ => List()
              }
              val then = primeRequest.then
              val result = then match {
                case Then(_, Some("read_request_timeout"), _) => ReadTimeout
                case Then(_, Some("unavailable"), _) => Unavailable
                case Then(_, Some("write_request_timeout"), _) => WriteTimeout
                case _ => Success
              }
              logger.debug("Column types " + primeRequest.then.column_types)
              val columnTypes = primeRequest.then.column_types match {
                case Some(types) => types.map({
                  case (key: String, value) => (key, ColumnTypeMapping.getOrElse(value, CqlVarchar))
                })
                case _ => Map[String, ColumnType]()
              }

              //check that all the columns in thr rows have a type
              val columnNamesInAllRows = resultsAsList.flatMap(row => row.keys).distinct

              val columnTypesWithMissingDefaultedToVarchar = columnNamesInAllRows.map( columnName => columnTypes.get(columnName) match {
                case Some(columnType) => (columnName, columnType)
                case None => (columnName, CqlVarchar)
              }).toMap

              primedResults.add(primeRequest.when, resultsAsList, result, columnTypesWithMissingDefaultedToVarchar)

              // all good
              StatusCodes.OK
            }
        }
      } ~
        delete {
          complete {
            logger.debug("Deleting all recorded priming")
            primedResults.clear()
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
