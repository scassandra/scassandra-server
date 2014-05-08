package uk.co.scassandra.priming.routes

import org.scalatest.{FunSpec, Matchers, FunSuite}
import spray.testkit.ScalatestRouteTest
import uk.co.scassandra.priming.{PrimingJsonImplicits}
import spray.http.StatusCodes
import uk.co.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle, PrimePreparedSingle}

class PrimingPreparedRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingPreparedRoute {

  implicit def actorRefFactory = system

  import PrimingJsonImplicits._

  describe("Priming") {
    it("Should take in query") {
      val when: WhenPreparedSingle = WhenPreparedSingle("select * from people where name = ?")
      val then: ThenPreparedSingle = ThenPreparedSingle(Some(List()));
      val prime = PrimePreparedSingle(when, then)
      Post("/prime-prepared-single", prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
      }
    }
  }
}
