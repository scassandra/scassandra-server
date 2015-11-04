package org.scassandra.server.actors

import akka.io.Tcp
import org.scalatest.FunSuite
import org.scassandra.server.cqlmessages.ProtocolProvider
import org.scassandra.server.priming.{ClosedConnectionResult, FatalResult}

trait FatalHandlingBehaviors extends ProtocolProvider {
  this: FunSuite =>

  def executeWithFatal(result: FatalResult, expectedCommand: Tcp.CloseCommand)

  test("Execute with ClosedCommand - close") {
    val result = ClosedConnectionResult("close")
    executeWithFatal(result, Tcp.Close)
  }

  test("Execute with ClosedCommand - reset") {
    val result = ClosedConnectionResult("reset")
    executeWithFatal(result, Tcp.Abort)
  }

  test("Execute with ClosedCommand - halfclose") {
    val result = ClosedConnectionResult("halfclose")
    executeWithFatal(result, Tcp.ConfirmedClose)
  }
}
