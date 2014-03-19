package uk.co.scassandra.priming

import org.scalatest._

import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest
import spray.json._

class PrimingServerTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with PrimingServerRoute {

  import JsonImplicits._

  implicit def actorRefFactory = system

  implicit val primedResults = PrimedResults()

  after {
    primedResults clear()
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


      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray], None)) ~> route ~> check {
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


      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray], None)) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults))
      }
    }

    it("should return populate PrimedResults with ReadTimeout when contained in Metadata") {
      val whenQuery = "select * from users"
      val thenResults = List[Map[String, String]]()
      val metadata = Metadata(Some("read_request_timeout"))

      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray], Some(metadata))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, ReadTimeout))
      }
    }

    it("should return populate PrimedResults with Success when contained in Metadata") {
      val whenQuery = "select * from users"
      val thenResults = List[Map[String, String]]()
      val metadata = Metadata(Some("success"))

      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray], Some(metadata))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, Success))
      }
    }

    it("should return populate PrimedResults with Unavailable when contained in Metadata") {
      val whenQuery = "select * from users"
      val thenResults = List[Map[String, String]]()
      val metadata = Metadata(Some("unavailable"))

      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray], Some(metadata))) ~> route ~> check {
        primedResults.get(whenQuery).get should equal(Prime(whenQuery, thenResults, Unavailable))
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
  }
}
