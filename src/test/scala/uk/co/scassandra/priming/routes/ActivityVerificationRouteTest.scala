package uk.co.scassandra.priming.routes

import org.scalatest.{Matchers, BeforeAndAfter, FunSpec}
import spray.testkit.ScalatestRouteTest
import uk.co.scassandra.priming.{PrimingJsonImplicits, Query, Connection, ActivityLog}
import spray.json.JsonParser
import uk.co.scassandra.cqlmessages.ONE

class ActivityVerificationRouteTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with ActivityVerificationRoute {

  implicit def actorRefFactory = system

  import PrimingJsonImplicits._

  describe("Retrieving activity") {
    it("Should return connection count from ActivityLog for single connection") {
      ActivityLog.clearConnections()
      ActivityLog.recordConnection()

      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(1)
      }
    }

    it("Should return connection count from ActivityLog for no connections") {
      ActivityLog.clearConnections()

      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(0)
      }
    }

    it("Should clear connections for a delete") {
      ActivityLog.recordConnection()

      Delete("/connection") ~> activityVerificationRoute ~> check {
        ActivityLog.retrieveConnections().size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - no queries") {
      ActivityLog.clearQueries()

      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - single query") {
      ActivityLog.clearQueries()
      val query: String = "select * from people"
      ActivityLog.recordQuery(query, ONE)

      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(1)
        queryList(0).query should equal(query)
      }
    }

    it("Should clear queries for a delete") {
      ActivityLog.recordQuery("select * from people", ONE)

      Delete("/query") ~> activityVerificationRoute ~> check {
        ActivityLog.retrieveQueries().size should equal(0)
      }
    }
  }
}
