package uk.co.scassandra.priming

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

class ActivityLogTest extends FunSuite with Matchers with BeforeAndAfter {

  before {
    ActivityLog.clearConnections()
    ActivityLog.clearQueries()
  }

  test("Clear connection activity log") {
    ActivityLog.recordConnection()
    ActivityLog.clearConnections()
    ActivityLog.retrieveConnections().size should equal(0)
  }

  test("Clear query activity log") {
    ActivityLog.recordQuery("select * from people")
    ActivityLog.clearQueries()
    ActivityLog.retrieveQueries().size should equal(0)
  }

  test("No connections should exist by default") {
    ActivityLog.retrieveConnections().size should equal(0)
  }

  test("Store connection and retrieve connection") {
    ActivityLog.recordConnection()
    ActivityLog.retrieveConnections().size should equal(1)
  }

  test("Store query and retrieve connection") {
    val query: String = "select * from people"
    ActivityLog.recordQuery(query)
    ActivityLog.retrieveQueries().size should equal(1)
    ActivityLog.retrieveQueries()(0).query should equal(query)
  }

}
