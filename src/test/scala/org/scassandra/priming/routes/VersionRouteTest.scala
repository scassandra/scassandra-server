package org.scassandra.priming.routes

import spray.testkit.ScalatestRouteTest
import org.scalatest.{Matchers, FunSpec}
import spray.http.StatusCodes._

class VersionRouteTest extends FunSpec with ScalatestRouteTest with VersionRoute with Matchers {
  implicit def actorRefFactory = system

  describe("Version info") {
    it("should get it from the implementation version") {
      Get("/version") ~> versionRoute ~> check {
        status should equal(OK)
      }
    }
  }
}
