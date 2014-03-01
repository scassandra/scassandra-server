package priming

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest
import spray.json._

class PrimingServerTest extends FunSpec with BeforeAndAfter with ShouldMatchers with ScalatestRouteTest with PrimingServerRoute {

  import JsonImplicits._

  implicit def actorRefFactory = system

  implicit val primedResults = PrimedResults()

  after {
    primedResults clear()
  }

  describe("Priming server") {
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


      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray])) ~> route ~> check {
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


      Post("/prime", PrimeQueryResult(whenQuery, thenResults.toJson.asInstanceOf[JsArray])) ~> route ~> check {
        primedResults get whenQuery should equal(thenResults)
      }
    }
  }
}
