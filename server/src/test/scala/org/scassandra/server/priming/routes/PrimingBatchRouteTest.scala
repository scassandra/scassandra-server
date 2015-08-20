package org.scassandra.server.priming.routes

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSpec}
import org.scassandra.server.priming.batch.{BatchWhen, BatchPrimeSingle, PrimeBatchStore}
import org.scassandra.server.priming.json.{PrimingJsonImplicits, Success}
import org.scassandra.server.priming.query.Then
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest

class PrimingBatchRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingBatchRoute with MockitoSugar {

  import PrimingJsonImplicits._

  implicit def actorRefFactory = system
  implicit val primeBatchStore: PrimeBatchStore = mock[PrimeBatchStore]
  val primeBatchSinglePath = "/prime-batch-single"

  describe("Priming") {
    it("Should store the prime in the prime store") {
      val when = BatchWhen(List())
      val thenDo = Then(rows = Some(List()), result = Some(Success))
      val prime = BatchPrimeSingle(when, thenDo)
      Post(primeBatchSinglePath, prime) ~> batchRoute ~> check {
        status should equal(StatusCodes.OK)
        verify(primeBatchStore).record(prime)
      }
    }
  }
}
