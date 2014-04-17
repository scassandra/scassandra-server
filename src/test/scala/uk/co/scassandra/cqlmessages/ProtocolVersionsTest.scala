package uk.co.scassandra.cqlmessages

import org.scalatest.{Matchers, FunSuite}
import org.scassandra.cqlmessages.ProtocolVersions

class ProtocolVersionsTest extends FunSuite with Matchers {
  test("Mapping client protocol version to server - version 1") {
    ProtocolVersions.serverVersionForClientVersion(ProtocolVersions.ClientProtocolVersionOne) should equal(ProtocolVersions.ServerProtocolVersionOne)
  }

  test("Mapping client protocol version to server - version 2") {
    ProtocolVersions.serverVersionForClientVersion(ProtocolVersions.ClientProtocolVersionTwo) should equal(ProtocolVersions.ServerProtocolVersionTwo)
  }
}
