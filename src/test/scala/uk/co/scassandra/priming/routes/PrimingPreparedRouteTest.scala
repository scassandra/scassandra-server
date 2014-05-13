package uk.co.scassandra.priming.routes

import org.scalatest.{FunSpec, Matchers, FunSuite}
import spray.testkit.ScalatestRouteTest
import uk.co.scassandra.priming.{PrimingJsonImplicits}
import spray.http.StatusCodes
import uk.co.scassandra.priming.prepared.{PrimePreparedStore, ThenPreparedSingle, WhenPreparedSingle, PrimePreparedSingle}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class PrimingPreparedRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingPreparedRoute with MockitoSugar {

  implicit def actorRefFactory = system
  implicit val primePreparedStore : PrimePreparedStore = mock[PrimePreparedStore]
  val primePreparedSinglePath = "/prime-prepared-single"

  import PrimingJsonImplicits._

  describe("Priming") {
    it("Should take in query") {
      val when: WhenPreparedSingle = WhenPreparedSingle("select * from people where name = ?")
      val then: ThenPreparedSingle = ThenPreparedSingle(Some(List()));
      val prime = PrimePreparedSingle(when, then)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).record(prime)
      }
    }

    it("should allow primes to be deleted") {
      Delete(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).clear()
      }
    }
  }
}
