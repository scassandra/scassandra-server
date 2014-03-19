package uk.co.scassandra.priming

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

class ActivityLogTest extends FunSuite with Matchers with BeforeAndAfter {

  before {
    ActivityLog.clearConnections()
  }

  test("Clear of activity log") {
    ActivityLog.recordConnection()
    ActivityLog.clearConnections()
    ActivityLog.retrieveConnections().size should equal(0)
  }

  test("No connections should exist by default") {
    ActivityLog.retrieveConnections().size should equal(0)
  }

  test("Store connection and retrieve connection") {
    ActivityLog.recordConnection()
    ActivityLog.retrieveConnections().size should equal(1)
  }

}
