package uk.co.scassandra.priming

import org.scalatest._

import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest
import spray.json._
import com.batey.narinc.client.cqlmessages.{CqlVarchar, CqlInt, ColumnType}

class PrimingServerTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with PrimingServerRoute {

  import JsonImplicits._

  implicit def actorRefFactory = system

  implicit val primedResults = PrimedResults()

  after {
    primedResults.clear()
  }

  describe("Priming") {
    it("should return OK on valid request") {
      val whenQuery = "select * from users"
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )


      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults.toJson.asInstanceOf[JsArray])))) ~> route ~> check {
        status should equal(OK)
      }
    }

    it("should return populate PrimedResults on valid request") {
      val whenQuery = "select * from users"
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val defaultedColumnTypes = Map[String, ColumnType]("name" -> CqlVarchar, "age" -> CqlVarchar)

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults.toJson.asInstanceOf[JsArray])))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, Success, defaultedColumnTypes))
      }
    }

    it("should return populate PrimedResults with ReadTimeout when result is read_request_timeout") {
      val whenQuery = "select * from users"
      val thenResults = List[Map[String, String]]()
      val result = Some("read_request_timeout")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults.toJson.asInstanceOf[JsArray]), result))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, ReadTimeout))
      }
    }

    it("should return populate PrimedResults with WriteTimeout when result is write_request_timeout") {
      val whenQuery = "insert into something"
      val thenResults = List[Map[String, String]]()
      val result = Some("write_request_timeout")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults.toJson.asInstanceOf[JsArray]), result))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, WriteTimeout))
      }
    }

    it("should return populate PrimedResults with Success when contained in Metadata") {
      val whenQuery = "select * from users"
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults.toJson.asInstanceOf[JsArray]), result))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, Success))
      }
    }

    it("should return populate PrimedResults with Unavailable when contained in Metadata") {
      val whenQuery = "select * from users"
      val thenResults = List[Map[String, String]]()
      val result = Some("unavailable")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults.toJson.asInstanceOf[JsArray]), result))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, Unavailable))
      }
    }

    it("should delete all primes for a delete") {
      val query = "anything"
      primedResults.add(query, List())

      Delete("/prime") ~> route ~> check {
        primedResults.get(query) should equal(None)
      }
    }
  }
  
  describe("Priming of types") {
    it("Should convert int to ColumnType Int") {
      val whenQuery = "select * from users"
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map(
        "age" -> "int"
      )
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows.toJson.asInstanceOf[JsArray]), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenRows, Success, columnTypes = Map[String, ColumnType]("age" -> CqlInt)))
      }
    }

    it("Should default column types to varchar") {
      val whenQuery = "select * from users"
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map[String, String]()
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows.toJson.asInstanceOf[JsArray]), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenRows, Success, columnTypes = Map[String, ColumnType]("age" -> CqlVarchar)))
      }
    }
  }

  describe("Retrieving activity") {
    it("Should return connection count from ActivityLog for single connection") {
      ActivityLog.clearConnections()
      ActivityLog.recordConnection()

      Get("/connection") ~> route ~> check {
        val response : String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(1)
      }
    }

    it("Should return connection count from ActivityLog for no connections") {
      ActivityLog.clearConnections()

      Get("/connection") ~> route ~> check {
        val response : String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(0)
      }
    }

    it("Should clear connections for a delete") {
      ActivityLog.recordConnection()

      Delete("/connection") ~> route ~> check {
        ActivityLog.retrieveConnections().size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - no queries") {
      ActivityLog.clearQueries()

      Get("/query") ~> route ~> check {
        val response : String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - single query") {
      ActivityLog.clearQueries()
      val query: String = "select * from people"
      ActivityLog.recordQuery(query)

      Get("/query") ~> route ~> check {
        val response : String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(1)
        queryList(0).query should equal(query)
      }
    }

    it("Should clear queries for a delete") {
      ActivityLog.recordQuery("select * from people")

      Delete("/query") ~> route ~> check {
        ActivityLog.retrieveQueries().size should equal(0)
      }
    }
  }
}
